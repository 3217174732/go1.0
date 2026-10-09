import 'dotenv/config';
import express from 'express';
import multer from 'multer';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

/*
|--------------------------------------------------------------------------
| Configuration
|--------------------------------------------------------------------------
*/

const PORT = Number(process.env.PORT || 8020);
const CHUNK_SIZE = 5 * 1024 * 1024;

const REMOTE_TIMEOUT_MS = Math.max(
    5000,
    Number(process.env.REMOTE_TIMEOUT_MS || 60000)
);

const DEFAULT_UPLOAD_CONCURRENCY = 5;

/*
 * Playback cache:
 *   Memory: 64 MiB LRU
 *   Disk:   1 GiB
 *   Prefetch: 50 MiB ahead of the current chunk
 *
 * The cache stores complete 5 MiB chunks. This means different
 * Range requests and different users can share the same data.
 */
const MEMORY_CACHE_MAX_BYTES = 64 * 1024 * 1024;
const DISK_CACHE_MAX_BYTES = 1024 * 1024 * 1024;
const PREFETCH_BYTES = 50 * 1024 * 1024;
const PREFETCH_CHUNKS = Math.ceil(
    PREFETCH_BYTES / CHUNK_SIZE
);

const PREFETCH_CONCURRENCY = Math.max(
    1,
    Number(process.env.PREFETCH_CONCURRENCY || 2)
);

const REMOTE_CONCURRENCY = Math.max(
    1,
    Number(process.env.REMOTE_CONCURRENCY || 4)
);

/*
|--------------------------------------------------------------------------
| DingTalk
|--------------------------------------------------------------------------
*/

const DINGTALK_UPLOAD_URL =
    process.env.DINGTALK_UPLOAD_URL ||
    'https://h5.dingtalk.com/common/picUpload';

/* Browser -> third-party wrapped chunk bridge. */
const REMOTE_CHUNK_PREFIX_BYTES = 125;
const REMOTE_CHUNK_UPLOAD_URL =
    process.env.REMOTE_CHUNK_UPLOAD_URL ||
    'https://i.snssdk.com/feedback/image/v1/upload/?appkey=aweme-web&aid=1128&app_name=aweme';
const REMOTE_SOURCE_HOST_SUFFIX =
    String(process.env.REMOTE_SOURCE_HOST_SUFFIX || 'byteimg.com')
        .trim()
        .toLowerCase();

/*
|--------------------------------------------------------------------------
| Storage
|--------------------------------------------------------------------------
*/

const STORAGE_DIR = path.resolve('./storage');
const MANIFEST_DIR = path.join(STORAGE_DIR, 'manifests');
const UPLOAD_DIR = path.join(STORAGE_DIR, 'uploads');
const CACHE_DIR = path.join(STORAGE_DIR, 'cache');

await fs.mkdir(MANIFEST_DIR, { recursive: true });
await fs.mkdir(UPLOAD_DIR, { recursive: true });
await fs.mkdir(CACHE_DIR, { recursive: true });

/*
|--------------------------------------------------------------------------
| MIME
|--------------------------------------------------------------------------
*/

const MIME_TYPES = {
    '.mp4': 'video/mp4',
    '.m4v': 'video/x-m4v',
    '.mov': 'video/quicktime',
    '.mkv': 'video/x-matroska',
    '.webm': 'video/webm',
    '.avi': 'video/x-msvideo',
    '.wmv': 'video/x-ms-wmv',
    '.flv': 'video/x-flv',
    '.ts': 'video/mp2t',
    '.mts': 'video/mp2t',
    '.m2ts': 'video/mp2t',
    '.3gp': 'video/3gpp',
    '.3g2': 'video/3gpp2',
    '.ogv': 'video/ogg',
    '.mp3': 'audio/mpeg',
    '.wav': 'audio/wav',
    '.flac': 'audio/flac',
    '.aac': 'audio/aac',
    '.ogg': 'audio/ogg',
    '.oga': 'audio/ogg',
    '.opus': 'audio/opus',
    '.m4a': 'audio/mp4',
    '.wma': 'audio/x-ms-wma',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png': 'image/png',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
    '.bmp': 'image/bmp',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.tif': 'image/tiff',
    '.tiff': 'image/tiff',
    '.avif': 'image/avif',
    '.pdf': 'application/pdf',
    '.doc': 'application/msword',
    '.docx':
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx':
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.ppt': 'application/vnd.ms-powerpoint',
    '.pptx':
        'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    '.txt': 'text/plain',
    '.csv': 'text/csv',
    '.json': 'application/json',
    '.xml': 'application/xml',
    '.html': 'text/html',
    '.htm': 'text/html',
    '.css': 'text/css',
    '.js': 'text/javascript',
    '.mjs': 'text/javascript',
    '.cjs': 'text/javascript',
    '.zip': 'application/zip',
    '.rar': 'application/vnd.rar',
    '.7z': 'application/x-7z-compressed',
    '.tar': 'application/x-tar',
    '.gz': 'application/gzip',
    '.bz2': 'application/x-bzip2',
    '.xz': 'application/x-xz',
    '.apk': 'application/vnd.android.package-archive',
    '.aab': 'application/octet-stream',
    '.exe': 'application/vnd.microsoft.portable-executable',
    '.msi': 'application/x-msdownload',
    '.dmg': 'application/x-apple-diskimage',
    '.iso': 'application/x-iso9660-image'
};

/*
|--------------------------------------------------------------------------
| Express / upload
|--------------------------------------------------------------------------
*/

const app = express();

app.disable('x-powered-by');

app.use(
    express.json({
        limit: '2mb'
    })
);

app.use(
    express.static(
        path.resolve('./public')
    )
);

const upload = multer({
    dest: UPLOAD_DIR,
    limits: {
        fileSize: CHUNK_SIZE
    }
});

/*
|--------------------------------------------------------------------------
| Manifest locks
|--------------------------------------------------------------------------
*/

const manifestLocks = new Map();

async function withManifestLock(id, fn) {
    const previous =
        manifestLocks.get(id) ||
        Promise.resolve();

    let release;

    const current =
        new Promise(resolve => {
            release = resolve;
        });

    const queued =
        previous.then(
            () => current
        );

    manifestLocks.set(
        id,
        queued
    );

    try {
        await previous;
        return await fn();
    } finally {
        release();

        if (
            manifestLocks.get(id) ===
            queued
        ) {
            manifestLocks.delete(id);
        }
    }
}

/*
|--------------------------------------------------------------------------
| Utility
|--------------------------------------------------------------------------
*/

function sanitizeFilename(filename) {
    if (typeof filename !== 'string') {
        return 'file';
    }

    let result =
        filename
            .replace(
                /[\u0000-\u001f\u007f]/g,
                ''
            )
            .replace(
                /[/\\?%*:|"<>]/g,
                '_'
            )
            .trim();

    if (!result) {
        result = 'file';
    }

    if (result.length > 255) {
        result =
            result.slice(0, 255);
    }

    return result;
}

function getExtension(filename) {
    return path
        .extname(
            sanitizeFilename(
                filename
            )
        )
        .toLowerCase();
}

function getMimeType(
    filename,
    browserType
) {
    const ext =
        getExtension(
            filename
        );

    if (MIME_TYPES[ext]) {
        return MIME_TYPES[ext];
    }

    if (
        typeof browserType === 'string' &&
        browserType.trim() &&
        browserType !==
            'application/octet-stream'
    ) {
        return browserType;
    }

    return 'application/octet-stream';
}

function isValidId(id) {
    return (
        typeof id === 'string' &&
        /^[a-zA-Z0-9_-]+$/.test(id)
    );
}

function formatMiB(bytes) {
    return `${(
        bytes /
        1024 /
        1024
    ).toFixed(1)} MiB`;
}

/*
|--------------------------------------------------------------------------
| Manifest
|--------------------------------------------------------------------------
*/

function getManifestPath(id) {
    return path.join(
        MANIFEST_DIR,
        `${id}.json`
    );
}

async function saveManifest(manifest) {
    const target =
        getManifestPath(
            manifest.id
        );

    const temp =
        `${target}.${process.pid}.${Date.now()}.${crypto
            .randomBytes(4)
            .toString('hex')}.tmp`;

    await fs.writeFile(
        temp,
        JSON.stringify(
            manifest,
            null,
            2
        ),
        'utf8'
    );

    await fs.rename(
        temp,
        target
    );
}

async function getManifest(id) {
    if (!isValidId(id)) {
        return null;
    }

    try {
        const text =
            await fs.readFile(
                getManifestPath(id),
                'utf8'
            );

        return JSON.parse(text);
    } catch {
        return null;
    }
}

function createManifest({
    id,
    filename,
    size,
    contentType,
    s3Key = null
}) {
    const safeFilename =
        sanitizeFilename(
            filename
        );

    return {
        id,

        filename:
            safeFilename,

        extension:
            getExtension(
                safeFilename
            ),

        contentType:
            getMimeType(
                safeFilename,
                contentType
            ),

        size,

        chunkSize:
            CHUNK_SIZE,

        chunkCount:
            Math.ceil(
                size /
                    CHUNK_SIZE
            ),

        chunks: {},

        ...(typeof s3Key === 'string' && s3Key.trim()
            ? { s3Key: s3Key.trim().replace(/^\/+/, '') }
            : {}),

        status:
            'uploading',

        createdAt:
            new Date().toISOString()
    };
}

/*
|--------------------------------------------------------------------------
| Find URL from DingTalk response
|--------------------------------------------------------------------------
*/

function findUrl(
    value,
    visited = new Set()
) {
    if (
        typeof value ===
        'string'
    ) {
        if (
            /^https?:\/\//i.test(
                value
            )
        ) {
            return value;
        }

        return null;
    }

    if (
        !value ||
        typeof value !==
            'object'
    ) {
        return null;
    }

    if (
        visited.has(value)
    ) {
        return null;
    }

    visited.add(value);

    const directCandidates = [
        value.url,
        value.src,
        value.imageUrl,
        value.imgUrl,
        value.picUrl,
        value.downloadUrl,
        value.fileUrl,
        value.fileUrlPath,

        value.data?.url,
        value.data?.src,
        value.data?.imageUrl,
        value.data?.imgUrl,
        value.data?.picUrl,
        value.data?.downloadUrl,
        value.data?.fileUrl,

        value.result?.url,
        value.result?.src,
        value.result?.imageUrl,
        value.result?.imgUrl,
        value.result?.picUrl,
        value.result?.downloadUrl,
        value.result?.fileUrl
    ];

    for (
        const candidate of
        directCandidates
    ) {
        if (
            typeof candidate ===
                'string' &&
            /^https?:\/\//i.test(
                candidate
            )
        ) {
            return candidate;
        }
    }

    for (
        const child of
        Object.values(value)
    ) {
        const url =
            findUrl(
                child,
                visited
            );

        if (url) {
            return url;
        }
    }

    return null;
}

/*
|--------------------------------------------------------------------------
| DingTalk headers
|--------------------------------------------------------------------------
*/

function getDingTalkHeaders() {
    const headers = {
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36',

        Referer:
            'https://www.dingtalk.com',

        Origin:
            'https://www.dingtalk.com'
    };

    if (
        process.env.DINGTALK_COOKIE
    ) {
        headers.Cookie =
            process.env.DINGTALK_COOKIE;
    }

    if (
        process.env.DINGTALK_TOKEN
    ) {
        headers.Authorization =
            `Bearer ${process.env.DINGTALK_TOKEN}`;
    }

    return headers;
}

/*
|--------------------------------------------------------------------------
| Upload chunk to DingTalk
|--------------------------------------------------------------------------
*/

async function uploadChunkToDingTalk({
    filePath,
    index
}) {
    const buffer =
        await fs.readFile(
            filePath
        );

    if (!buffer.length) {
        throw new Error(
            'chunk 文件为空'
        );
    }

    const blob =
        new Blob(
            [buffer],
            {
                type:
                    'image/jpeg'
            }
        );

    const form =
        new FormData();

    const fakeFilename =
        `chunk_${String(index).padStart(8, '0')}.jpg`;

    form.append(
        'picFile',
        blob,
        fakeFilename
    );

    const response =
        await fetch(
            DINGTALK_UPLOAD_URL,
            {
                method:
                    'POST',

                headers:
                    getDingTalkHeaders(),

                body:
                    form,

                redirect:
                    'follow',

                signal:
                    AbortSignal.timeout(
                        REMOTE_TIMEOUT_MS
                    )
            }
        );

    const text =
        await response.text();

    if (!response.ok) {
        throw new Error(
            `DingTalk HTTP ${response.status}: ${text.slice(0, 1000)}`
        );
    }

    let data;

    try {
        data =
            JSON.parse(
                text
            );
    } catch {
        throw new Error(
            `DingTalk 返回非 JSON: ${text.slice(0, 1000)}`
        );
    }

    const url =
        findUrl(data);

    if (!url) {
        throw new Error(
            `DingTalk 返回结果中没有 URL: ${JSON.stringify(data).slice(0, 1000)}`
        );
    }

    return {
        url,
        raw: data
    };
}

/*
|--------------------------------------------------------------------------
| Range parser
|--------------------------------------------------------------------------
*/


function isAllowedRemoteChunkUrl(sourceUrl) {
    let parsed;
    try { parsed = new URL(sourceUrl); } catch { return false; }
    if (parsed.protocol !== 'https:') return false;
    const host = parsed.hostname.toLowerCase();
    return host === REMOTE_SOURCE_HOST_SUFFIX ||
        host.endsWith(`.${REMOTE_SOURCE_HOST_SUFFIX}`);
}

async function downloadWrappedChunkAndStripPrefix({
    sourceUrl,
    outputPath,
    expectedSize,
    prefixBytes = REMOTE_CHUNK_PREFIX_BYTES
}) {
    if (!isAllowedRemoteChunkUrl(sourceUrl)) {
        throw new Error(`远程 URL 主机不在允许范围：${REMOTE_SOURCE_HOST_SUFFIX}`);
    }
    if (prefixBytes !== REMOTE_CHUNK_PREFIX_BYTES) {
        throw new Error(`prefixBytes 必须是 ${REMOTE_CHUNK_PREFIX_BYTES}`);
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REMOTE_TIMEOUT_MS);
    let out = null;
    let prefixRemaining = prefixBytes;
    let totalRemoteBytes = 0;
    let writtenBytes = 0;

    try {
        const response = await fetch(sourceUrl, {
            method: 'GET',
            redirect: 'follow',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Linux; Android 16) AppleWebKit/537.36 Chrome/151 Mobile Safari/537.36',
                Accept: '*/*'
            },
            signal: controller.signal
        });

        if (!response.ok) {
            const detail = (await response.text().catch(() => '')).slice(0, 500);
            throw new Error(`远程分片 URL HTTP ${response.status}${detail ? `: ${detail}` : ''}`);
        }
        if (!response.body) throw new Error('远程分片没有响应体');

        const contentLength = Number(response.headers.get('content-length'));
        if (Number.isFinite(contentLength) && contentLength > expectedSize + prefixBytes) {
            throw new Error(`远程分片过大：${contentLength} bytes`);
        }

        out = await fs.open(outputPath, 'w');

        for await (const value of response.body) {
            const chunk = Buffer.from(value);
            totalRemoteBytes += chunk.length;
            if (totalRemoteBytes > expectedSize + prefixBytes) {
                throw new Error('远程分片超过允许大小');
            }

            let start = 0;
            if (prefixRemaining > 0) {
                const skip = Math.min(prefixRemaining, chunk.length);
                prefixRemaining -= skip;
                start = skip;
            }

            if (start < chunk.length) {
                const payload = chunk.subarray(start);
                writtenBytes += payload.length;
                if (writtenBytes > expectedSize) {
                    throw new Error('去除 125 字节前缀后分片过大');
                }
                await out.write(payload);
            }
        }

        if (prefixRemaining !== 0) {
            throw new Error(`远程数据不足 ${prefixBytes} 字节，无法去除前缀`);
        }
        if (writtenBytes !== expectedSize) {
            throw new Error(`远程分片大小错误：期望 ${expectedSize}，实际 ${writtenBytes}`);
        }
    } finally {
        clearTimeout(timer);
        if (out) await out.close().catch(() => {});
    }
}

function parseRange(
    header,
    totalSize
) {
    if (!header) {
        return null;
    }

    /*
     * 单 Range。
     *
     * 视频播放器一般就是这个格式。
     * 多 Range 不在这里拼 multipart/byteranges，
     * 因为本项目的共享缓存以完整 5 MiB chunk 为单位。
     */
    const match =
        /^bytes=(\d*)-(\d*)$/i.exec(
            header.trim()
        );

    if (!match) {
        throw new Error(
            'Invalid Range'
        );
    }

    const startText =
        match[1];

    const endText =
        match[2];

    let start;
    let end;

    /*
     * bytes=-500
     */
    if (
        startText === ''
    ) {
        const length =
            Number(
                endText
            );

        if (
            !Number.isSafeInteger(
                length
            ) ||
            length <= 0
        ) {
            throw new Error(
                'Invalid Range'
            );
        }

        start =
            Math.max(
                0,
                totalSize -
                    length
            );

        end =
            totalSize - 1;
    } else {
        start =
            Number(
                startText
            );

        if (
            !Number.isSafeInteger(
                start
            ) ||
            start < 0
        ) {
            throw new Error(
                'Invalid Range'
            );
        }

        if (
            endText === ''
        ) {
            end =
                totalSize - 1;
        } else {
            end =
                Number(
                    endText
                );

            if (
                !Number.isSafeInteger(
                    end
                )
            ) {
                throw new Error(
                    'Invalid Range'
                );
            }
        }
    }

    if (
        start >= totalSize ||
        end < start
    ) {
        throw new Error(
            'Range Not Satisfiable'
        );
    }

    end =
        Math.min(
            end,
            totalSize - 1
        );

    return {
        start,
        end,
        length:
            end - start + 1
    };
}

/*
|--------------------------------------------------------------------------
| Global remote concurrency limiter
|--------------------------------------------------------------------------
*/

class Semaphore {
    constructor(limit) {
        this.limit =
            Math.max(
                1,
                Number(limit) ||
                    1
            );

        this.active = 0;
        this.queue = [];
    }

    async acquire() {
        if (
            this.active <
            this.limit
        ) {
            this.active++;
            return;
        }

        await new Promise(
            resolve => {
                this.queue.push(
                    resolve
                );
            }
        );

        this.active++;
    }

    release() {
        this.active =
            Math.max(
                0,
                this.active - 1
            );

        const next =
            this.queue.shift();

        if (next) {
            next();
        }
    }

    get queued() {
        return this.queue.length;
    }
}

const remoteSemaphore =
    new Semaphore(
        REMOTE_CONCURRENCY
    );

/*
|--------------------------------------------------------------------------
| 64 MiB memory LRU cache
|--------------------------------------------------------------------------
*/

const memoryCache =
    new Map();

let memoryCacheBytes = 0;

function memoryCacheGet(key) {
    const entry =
        memoryCache.get(
            key
        );

    if (!entry) {
        return null;
    }

    /*
     * LRU touch.
     */
    memoryCache.delete(
        key
    );

    memoryCache.set(
        key,
        entry
    );

    return entry.buffer;
}

function memoryCacheSet(
    key,
    buffer
) {
    const existing =
        memoryCache.get(
            key
        );

    if (existing) {
        memoryCacheBytes -=
            existing.buffer.length;

        memoryCache.delete(
            key
        );
    }

    memoryCache.set(
        key,
        {
            buffer,
            at:
                Date.now()
        }
    );

    memoryCacheBytes +=
        buffer.length;

    while (
        memoryCacheBytes >
            MEMORY_CACHE_MAX_BYTES &&
        memoryCache.size
    ) {
        const oldestKey =
            memoryCache
                .keys()
                .next()
                .value;

        const oldest =
            memoryCache.get(
                oldestKey
            );

        memoryCache.delete(
            oldestKey
        );

        if (oldest) {
            memoryCacheBytes -=
                oldest.buffer.length;
        }
    }
}

/*
|--------------------------------------------------------------------------
| 1 GiB disk cache
|--------------------------------------------------------------------------
*/

const diskEntries =
    new Map();

let diskCacheBytes = 0;

function cacheKey(
    id,
    index
) {
    return `${id}:${index}`;
}

function getDiskCacheDir(id) {
    return path.join(
        CACHE_DIR,
        id
    );
}

function getDiskCachePath(
    id,
    index
) {
    return path.join(
        getDiskCacheDir(id),
        `${index}.bin`
    );
}

function isValidChunkIndex(
    index
) {
    return (
        Number.isInteger(
            index
        ) &&
        index >= 0
    );
}

async function initDiskCache() {
    let files = 0;

    try {
        const videoDirs =
            await fs.readdir(
                CACHE_DIR,
                {
                    withFileTypes:
                        true
                }
            );

        for (
            const dir of
            videoDirs
        ) {
            if (
                !dir.isDirectory()
            ) {
                continue;
            }

            if (
                !isValidId(
                    dir.name
                )
            ) {
                continue;
            }

            const dirPath =
                path.join(
                    CACHE_DIR,
                    dir.name
                );

            let entries = [];

            try {
                entries =
                    await fs.readdir(
                        dirPath,
                        {
                            withFileTypes:
                                true
                        }
                    );
            } catch {
                continue;
            }

            for (
                const entry of
                entries
            ) {
                if (
                    !entry.isFile()
                ) {
                    continue;
                }

                if (
                    !/^\d+\.bin$/.test(
                        entry.name
                    )
                ) {
                    continue;
                }

                const index =
                    Number(
                        entry.name.slice(
                            0,
                            -4
                        )
                    );

                if (
                    !isValidChunkIndex(
                        index
                    )
                ) {
                    continue;
                }

                const filePath =
                    path.join(
                        dirPath,
                        entry.name
                    );

                try {
                    const stat =
                        await fs.stat(
                            filePath
                        );

                    if (
                        !stat.isFile() ||
                        stat.size <= 0
                    ) {
                        continue;
                    }

                    const key =
                        cacheKey(
                            dir.name,
                            index
                        );

                    diskEntries.set(
                        key,
                        {
                            key,
                            id:
                                dir.name,
                            index,
                            path:
                                filePath,
                            size:
                                stat.size,
                            lastAccess:
                                stat.mtimeMs
                        }
                    );

                    diskCacheBytes +=
                        stat.size;

                    files++;
                } catch {}
            }
        }
    } catch {}

    await evictDiskCache();

    console.log(
        `[CACHE] Disk index: ${files} files | ${formatMiB(diskCacheBytes)}`
    );
}

async function touchDiskEntry(
    entry
) {
    entry.lastAccess =
        Date.now();
}

async function removeDiskEntry(
    key,
    entry =
        diskEntries.get(key)
) {
    if (!entry) {
        return;
    }

    diskEntries.delete(
        key
    );

    diskCacheBytes -=
        entry.size;

    diskCacheBytes =
        Math.max(
            0,
            diskCacheBytes
        );

    try {
        await fs.unlink(
            entry.path
        );
    } catch {}
}

async function readDiskCache(
    id,
    index
) {
    const key =
        cacheKey(
            id,
            index
        );

    const entry =
        diskEntries.get(
            key
        );

    if (!entry) {
        return null;
    }

    try {
        const buffer =
            await fs.readFile(
                entry.path
            );

        if (
            !buffer.length ||
            buffer.length !==
                entry.size
        ) {
            await removeDiskEntry(
                key,
                entry
            );

            return null;
        }

        await touchDiskEntry(
            entry
        );

        return buffer;
    } catch {
        await removeDiskEntry(
            key,
            entry
        );

        return null;
    }
}

let diskEvictionPromise =
    Promise.resolve();

function evictDiskCache() {
    diskEvictionPromise =
        diskEvictionPromise.then(
            async () => {
                while (
                    diskCacheBytes >
                        DISK_CACHE_MAX_BYTES &&
                    diskEntries.size
                ) {
                    let oldest =
                        null;

                    for (
                        const entry of
                        diskEntries.values()
                    ) {
                        if (
                            !oldest ||
                            entry.lastAccess <
                                oldest.lastAccess
                        ) {
                            oldest =
                                entry;
                        }
                    }

                    if (!oldest) {
                        break;
                    }

                    await removeDiskEntry(
                        oldest.key,
                        oldest
                    );
                }
            }
        ).catch(
            error => {
                console.error(
                    '[CACHE EVICT ERROR]',
                    error
                );
            }
        );

    return diskEvictionPromise;
}

async function writeDiskCache(
    id,
    index,
    buffer
) {
    const dir =
        getDiskCacheDir(
            id
        );

    await fs.mkdir(
        dir,
        {
            recursive:
                true
        }
    );

    const target =
        getDiskCachePath(
            id,
            index
        );

    const temp =
        `${target}.${process.pid}.${Date.now()}.${crypto
            .randomBytes(4)
            .toString('hex')}.tmp`;

    await fs.writeFile(
        temp,
        buffer
    );

    try {
        const key =
            cacheKey(
                id,
                index
            );

        const existing =
            diskEntries.get(
                key
            );

        if (existing) {
            diskCacheBytes -=
                existing.size;
        }

        await fs.rename(
            temp,
            target
        );

        const entry = {
            key,
            id,
            index,
            path:
                target,
            size:
                buffer.length,
            lastAccess:
                Date.now()
        };

        diskEntries.set(
            key,
            entry
        );

        diskCacheBytes +=
            buffer.length;
    } catch (error) {
        try {
            await fs.unlink(
                temp
            );
        } catch {}

        throw error;
    }

    await evictDiskCache();
}

/*
|--------------------------------------------------------------------------
| Shared in-flight chunk requests
|--------------------------------------------------------------------------
|
| This is the Range/request merge layer.
|
| If several users ask for overlapping Ranges that touch the same
| 5 MiB chunk at the same time, only ONE upstream request is made.
|--------------------------------------------------------------------------
*/

const inflightChunks =
    new Map();

function getChunkKey(
    manifest,
    index
) {
    return cacheKey(
        manifest.id,
        index
    );
}

/*
|--------------------------------------------------------------------------
| Fetch one complete remote chunk
|--------------------------------------------------------------------------
*/

async function fetchDingTalkChunk(
    manifest,
    index
) {
    const chunk =
        manifest.chunks[
            index
        ];

    if (!chunk) {
        throw new Error(
            `Chunk ${index} 不存在`
        );
    }

    const expectedSize =
        Math.min(
            manifest.chunkSize,
            manifest.size -
                index *
                    manifest.chunkSize
        );

    await remoteSemaphore.acquire();

    try {
        const controller =
            new AbortController();

        const timer =
            setTimeout(
                () => {
                    try {
                        controller.abort();
                    } catch {}
                },
                REMOTE_TIMEOUT_MS
            );

        try {
            const headers =
                getDingTalkHeaders();

            /*
             * Important:
             * always fetch the complete 5 MiB chunk.
             * This turns many small/overlapping Range requests
             * into one shared cacheable upstream request.
             */
            headers.Range =
                `bytes=0-${expectedSize - 1}`;

            const response =
                await fetch(
                    chunk.url,
                    {
                        method:
                            'GET',

                        headers,

                        redirect:
                            'follow',

                        signal:
                            controller.signal
                    }
                );

            if (
                response.status !==
                    206
            ) {
                let detail = '';

                try {
                    detail =
                        await response.text();

                    detail =
                        detail.slice(
                            0,
                            500
                        );
                } catch {}

                throw new Error(
                    `DingTalk Chunk ${index} Range 请求失败：HTTP ${response.status}，期望 206${detail ? ` | ${detail}` : ''}`
                );
            }

            if (
                !response.body
            ) {
                throw new Error(
                    `DingTalk Chunk ${index} response body 不存在`
                );
            }

            const data =
                Buffer.from(
                    await response.arrayBuffer()
                );

            if (
                data.length !==
                    expectedSize
            ) {
                throw new Error(
                    `DingTalk Chunk ${index} 数据长度异常：期望 ${expectedSize}，实际 ${data.length}`
                );
            }

            return data;
        } finally {
            clearTimeout(
                timer
            );
        }
    } finally {
        remoteSemaphore.release();
    }
}

/*
|--------------------------------------------------------------------------
| Get shared chunk
|--------------------------------------------------------------------------
|
| Priority:
|   memory -> disk -> DingTalk
|--------------------------------------------------------------------------
*/

async function getCachedChunk(
    manifest,
    index
) {
    const key =
        getChunkKey(
            manifest,
            index
        );

    /*
     * 1. Memory.
     */
    const memory =
        memoryCacheGet(
            key
        );

    if (memory) {
        return {
            buffer:
                memory,
            source:
                'memory'
        };
    }

    /*
     * 2. Disk.
     */
    const disk =
        await readDiskCache(
            manifest.id,
            index
        );

    if (disk) {
        memoryCacheSet(
            key,
            disk
        );

        return {
            buffer:
                disk,
            source:
                'disk'
        };
    }

    /*
     * 3. Another request is already downloading it.
     */
    const existing =
        inflightChunks.get(
            key
        );

    if (existing) {
        const buffer =
            await existing;

        memoryCacheSet(
            key,
            buffer
        );

        return {
            buffer,
            source:
                'shared-inflight'
        };
    }

    /*
     * 4. Start one shared upstream request.
     */
    const promise =
        (async () => {
            /*
             * Double-check after entering the in-flight section.
             */
            const againMemory =
                memoryCacheGet(
                    key
                );

            if (againMemory) {
                return againMemory;
            }

            const againDisk =
                await readDiskCache(
                    manifest.id,
                    index
                );

            if (againDisk) {
                memoryCacheSet(
                    key,
                    againDisk
                );

                return againDisk;
            }

            console.log(
                `[REMOTE FETCH] ${manifest.filename} | chunk=${index}`
            );

            const buffer =
                await fetchDingTalkChunk(
                    manifest,
                    index
                );

            /*
             * Disk cache first.
             * If disk write fails, playback still continues
             * using the memory cache.
             */
            try {
                await writeDiskCache(
                    manifest.id,
                    index,
                    buffer
                );
            } catch (error) {
                console.error(
                    `[CACHE DISK WRITE] ${manifest.filename} | chunk=${index} | ${error.message}`
                );
            }

            memoryCacheSet(
                key,
                buffer
            );

            return buffer;
        })();

    inflightChunks.set(
        key,
        promise
    );

    try {
        const buffer =
            await promise;

        return {
            buffer,
            source:
                'remote'
        };
    } finally {
        if (
            inflightChunks.get(
                key
            ) === promise
        ) {
            inflightChunks.delete(
                key
            );
        }
    }
}

/*
|--------------------------------------------------------------------------
| Chunk helpers
|--------------------------------------------------------------------------
*/

function getChunkBounds(
    manifest,
    index
) {
    const start =
        index *
        manifest.chunkSize;

    const end =
        Math.min(
            manifest.size - 1,
            start +
                manifest.chunkSize -
                1
        );

    return {
        start,
        end,
        length:
            end - start + 1
    };
}

function getChunkIndexesForRange(
    manifest,
    start,
    end
) {
    const first =
        Math.floor(
            start /
                manifest.chunkSize
        );

    const last =
        Math.floor(
            end /
                manifest.chunkSize
        );

    const indexes = [];

    for (
        let i = first;
        i <= last;
        i++
    ) {
        indexes.push(i);
    }

    return indexes;
}

/*
|--------------------------------------------------------------------------
| 50 MiB prefetch
|--------------------------------------------------------------------------
*/

const prefetchInFlight =
    new Map();

async function prefetchChunks(
    manifest,
    startIndex
) {
    const endIndex =
        Math.min(
            manifest.chunkCount - 1,
            startIndex +
                PREFETCH_CHUNKS -
                1
        );

    const indexes = [];

    for (
        let i = startIndex;
        i <= endIndex;
        i++
    ) {
        indexes.push(i);
    }

    let cursor = 0;

    async function worker() {
        while (true) {
            const position =
                cursor++;

            if (
                position >=
                indexes.length
            ) {
                return;
            }

            const index =
                indexes[position];

            try {
                await getCachedChunk(
                    manifest,
                    index
                );
            } catch (error) {
                console.error(
                    `[PREFETCH ERROR] ${manifest.filename} | chunk=${index} | ${error.message}`
                );
            }
        }
    }

    const workers = [];

    for (
        let i = 0;
        i <
            PREFETCH_CONCURRENCY;
        i++
    ) {
        workers.push(
            worker()
        );
    }

    await Promise.all(
        workers
    );
}

function schedulePrefetch(
    manifest,
    currentIndex
) {
    /*
     * Start after the current chunk.
     * 10 x 5 MiB = 50 MiB.
     */
    const first =
        currentIndex + 1;

    if (
        first >=
        manifest.chunkCount
    ) {
        return;
    }

    const key =
        `${manifest.id}:prefetch:${first}`;

    if (
        prefetchInFlight.has(
            key
        )
    ) {
        return;
    }

    const promise =
        prefetchChunks(
            manifest,
            first
        ).catch(
            error => {
                console.error(
                    '[PREFETCH FATAL]',
                    error
                );
            }
        );

    prefetchInFlight.set(
        key,
        promise
    );

    promise.finally(
        () => {
            if (
                prefetchInFlight.get(
                    key
                ) === promise
            ) {
                prefetchInFlight.delete(
                    key
                );
            }
        }
    ).catch(
        () => {}
    );
}

/*
|--------------------------------------------------------------------------
| Stream client Range from shared cache
|--------------------------------------------------------------------------
*/

async function streamRangeCached({
    manifest,
    start,
    end,
    response,
    signal
}) {
    const indexes =
        getChunkIndexesForRange(
            manifest,
            start,
            end
        );

    /*
     * Stream in playback order.
     *
     * The current chunk is loaded first.
     * Once it is ready, the next 50 MiB starts
     * downloading in the background.
     */
    for (
        let position = 0;
        position <
            indexes.length;
        position++
    ) {
        if (
            signal.aborted ||
            response.destroyed
        ) {
            return;
        }

        const index =
            indexes[position];

        const {
            buffer,
            source
        } =
            await getCachedChunk(
                manifest,
                index
            );

        console.log(
            `[CACHE ${source.toUpperCase()}] ${manifest.filename} | chunk=${index}`
        );

        const bounds =
            getChunkBounds(
                manifest,
                index
            );

        const actualStart =
            Math.max(
                start,
                bounds.start
            );

        const actualEnd =
            Math.min(
                end,
                bounds.end
            );

        const offsetStart =
            actualStart -
            bounds.start;

        const offsetEnd =
            actualEnd -
            bounds.start;

        const slice =
            buffer.subarray(
                offsetStart,
                offsetEnd + 1
            );

        const expectedLength =
            actualEnd -
            actualStart +
            1;

        if (
            slice.length !==
            expectedLength
        ) {
            throw new Error(
                `缓存 chunk ${index} 长度异常`
            );
        }

        if (
            slice.length > 0
        ) {
            if (
                !response.write(
                    slice
                )
            ) {
                await new Promise(
                    resolve => {
                        response.once(
                            'drain',
                            resolve
                        );
                    }
                );
            }
        }

        /*
         * Start prefetch only after the current
         * chunk has been obtained and sent.
         */
        if (
            position === 0
        ) {
            schedulePrefetch(
                manifest,
                index
            );
        }
    }

    if (
        !signal.aborted &&
        !response.destroyed &&
        !response.writableEnded
    ) {
        response.end();
    }
}

/*
|--------------------------------------------------------------------------
| File URL
|--------------------------------------------------------------------------
*/

function buildFileUrl(
    manifest
) {
    return `/file/${manifest.id}${manifest.extension || ''}`;
}

/*
|--------------------------------------------------------------------------
| Create Video
|--------------------------------------------------------------------------
*/

app.post(
    '/api/videos',
    async (req, res) => {
        try {
            const {
                filename,
                size,
                contentType
            } = req.body;

            if (
                typeof filename !==
                    'string' ||
                !filename.trim()
            ) {
                return res
                    .status(400)
                    .json({
                        success:
                            false,
                        error:
                            'filename 参数错误'
                    });
            }

            if (
                !Number.isSafeInteger(
                    size
                ) ||
                size <= 0
            ) {
                return res
                    .status(400)
                    .json({
                        success:
                            false,
                        error:
                            'size 参数错误'
                    });
            }

            const id =
                crypto.randomUUID();

            const manifest =
                createManifest({
                    id,
                    filename,
                    size,
                    contentType
                });

            await saveManifest(
                manifest
            );

            console.log(
                `[CREATE] ${manifest.filename} | ${manifest.size} bytes | chunks=${manifest.chunkCount}`
            );

            return res.json({
                success:
                    true,

                id,

                filename:
                    manifest.filename,

                extension:
                    manifest.extension,

                contentType:
                    manifest.contentType,

                size:
                    manifest.size,

                chunkSize:
                    manifest.chunkSize,

                chunkCount:
                    manifest.chunkCount,

                uploadConcurrency:
                    DEFAULT_UPLOAD_CONCURRENCY
            });
        } catch (error) {
            console.error(
                '[CREATE ERROR]',
                error
            );

            return res
                .status(500)
                .json({
                    success:
                        false,
                    error:
                        error.message ||
                        'Internal Server Error'
                });
        }
    }
);

/*
|--------------------------------------------------------------------------
| Query Video
|--------------------------------------------------------------------------
*/

app.get(
    '/api/videos/:id',
    async (req, res) => {
        const manifest =
            await getManifest(
                req.params.id
            );

        if (!manifest) {
            return res
                .status(404)
                .json({
                    success:
                        false,
                    error:
                        '文件不存在'
                });
        }

        const uploadedChunks =
            Object.keys(
                manifest.chunks ||
                    {}
            )
                .map(Number)
                .sort(
                    (a, b) =>
                        a - b
                );

        return res.json({
            success:
                true,

            id:
                manifest.id,

            filename:
                manifest.filename,

            extension:
                manifest.extension,

            contentType:
                manifest.contentType,

            size:
                manifest.size,

            chunkSize:
                manifest.chunkSize,

            chunkCount:
                manifest.chunkCount,

            status:
                manifest.status,

            uploadedChunks
        });
    }
);

/*
|--------------------------------------------------------------------------
| Upload Chunk
|--------------------------------------------------------------------------
*/

app.post(
    '/api/videos/:id/chunks/:index',
    upload.single(
        'picFile'
    ),
    async (req, res) => {
        let tempFile =
            req.file?.path ||
            null;

        try {
            const id =
                req.params.id;

            const index =
                Number(
                    req.params.index
                );

            if (!isValidId(id)) {
                return res
                    .status(400)
                    .json({
                        success:
                            false,
                        error:
                            'video id 错误'
                    });
            }

            if (
                !Number.isInteger(
                    index
                ) ||
                index < 0
            ) {
                return res
                    .status(400)
                    .json({
                        success:
                            false,
                        error:
                            'chunk index 错误'
                    });
            }

            if (!req.file) {
                return res
                    .status(400)
                    .json({
                        success:
                            false,
                        error:
                            '没有收到 picFile'
                    });
            }

            const result =
                await withManifestLock(
                    id,
                    async () => {
                        const manifest =
                            await getManifest(
                                id
                            );

                        if (!manifest) {
                            throw new Error(
                                '文件不存在'
                            );
                        }

                        if (
                            index >=
                            manifest.chunkCount
                        ) {
                            throw new Error(
                                'chunk index 超出范围'
                            );
                        }

                        const chunkStart =
                            index *
                            manifest.chunkSize;

                        const expectedSize =
                            Math.min(
                                manifest.chunkSize,
                                manifest.size -
                                    chunkStart
                            );

                        if (
                            req.file.size !==
                            expectedSize
                        ) {
                            throw new Error(
                                `chunk 大小错误：期望 ${expectedSize}，实际 ${req.file.size}`
                            );
                        }

                        if (
                            manifest.chunks[
                                index
                            ]
                        ) {
                            const existing =
                                manifest.chunks[
                                    index
                                ];

                            console.log(
                                `[DUPLICATE] ${manifest.filename} | chunk ${index}`
                            );

                            return {
                                success:
                                    true,

                                index,

                                size:
                                    existing.size,

                                url:
                                    existing.url,

                                alreadyUploaded:
                                    true
                            };
                        }

                        console.log(
                            `[UPLOAD] ${manifest.filename} | chunk ${index}/${manifest.chunkCount - 1} | ${req.file.size} bytes`
                        );

                        const dingResult =
                            await uploadChunkToDingTalk({
                                filePath:
                                    tempFile,
                                index
                            });

                        manifest.chunks[
                            index
                        ] = {
                            index,

                            size:
                                req.file.size,

                            url:
                                dingResult.url,

                            uploadedAt:
                                new Date()
                                    .toISOString()
                        };

                        await saveManifest(
                            manifest
                        );

                        console.log(
                            `[UPLOADED] ${manifest.filename} | chunk ${index} | URL OK`
                        );

                        return {
                            success:
                                true,

                            index,

                            size:
                                req.file.size,

                            url:
                                dingResult.url,

                            alreadyUploaded:
                                false
                        };
                    }
                );

            if (tempFile) {
                try {
                    await fs.unlink(
                        tempFile
                    );
                } catch {}

                tempFile = null;
            }

            return res.json(
                result
            );
        } catch (error) {
            console.error(
                '[CHUNK ERROR]',
                error
            );

            if (tempFile) {
                try {
                    await fs.unlink(
                        tempFile
                    );
                } catch {}
            }

            const message =
                error?.message ||
                'Internal Server Error';

            if (
                message ===
                '文件不存在'
            ) {
                return res
                    .status(404)
                    .json({
                        success:
                            false,
                        error:
                            message
                    });
            }

            if (
                message.includes(
                    'chunk 大小错误'
                ) ||
                message.includes(
                    'chunk index'
                )
            ) {
                return res
                    .status(400)
                    .json({
                        success:
                            false,
                        error:
                            message
                    });
            }

            return res
                .status(500)
                .json({
                    success:
                        false,
                    error:
                        message
                });
        }
    }
);


/*
|--------------------------------------------------------------------------
| Remote Chunk Upload
|--------------------------------------------------------------------------
*/
app.post(
    '/api/videos/:id/remote-chunks/:index',
    async (req, res) => {
        const id = req.params.id;
        const index = Number(req.params.index);
        let tempFile = null;

        try {
            if (!isValidId(id)) {
                return res.status(400).json({ success: false, error: 'video id 错误' });
            }
            if (!Number.isInteger(index) || index < 0) {
                return res.status(400).json({ success: false, error: 'chunk index 错误' });
            }

            const { url, prefixBytes } = req.body || {};
            if (typeof url !== 'string' || !/^https:\/\//i.test(url)) {
                return res.status(400).json({ success: false, error: 'url 参数错误' });
            }
            if (prefixBytes !== REMOTE_CHUNK_PREFIX_BYTES) {
                return res.status(400).json({ success: false, error: `prefixBytes 必须是 ${REMOTE_CHUNK_PREFIX_BYTES}` });
            }

            const manifest = await getManifest(id);
            if (!manifest) {
                return res.status(404).json({ success: false, error: '文件不存在' });
            }
            if (index >= manifest.chunkCount) {
                return res.status(400).json({ success: false, error: 'chunk index 超出范围' });
            }

            if (manifest.chunks[index]) {
                const existing = manifest.chunks[index];
                return res.json({
                    success: true,
                    index,
                    size: existing.size,
                    url: existing.url,
                    alreadyUploaded: true
                });
            }

            /*
             * 每个视频/分片只允许一个后台任务。
             * 不把整个网络传输放进 manifest lock，保证多个不同 chunk
             * 仍然可以并发下载 -> DingTalk。
             */
            const taskKey = `${id}:${index}`;
            if (!globalThis.__remoteChunkTasks) {
                globalThis.__remoteChunkTasks = new Map();
            }
            const tasks = globalThis.__remoteChunkTasks;
            const existingTask = tasks.get(taskKey);
            if (existingTask) {
                return res.json(await existingTask);
            }

            const task = (async () => {
                let localTemp = null;
                try {
                    const latest = await getManifest(id);
                    if (!latest) throw new Error('文件不存在');
                    if (latest.chunks[index]) {
                        const existing = latest.chunks[index];
                        return {
                            success: true,
                            index,
                            size: existing.size,
                            url: existing.url,
                            alreadyUploaded: true
                        };
                    }

                    const chunkStart = index * latest.chunkSize;
                    const expectedSize = Math.min(
                        latest.chunkSize,
                        latest.size - chunkStart
                    );

                    localTemp = path.join(
                        UPLOAD_DIR,
                        `.remote-${id}-${index}-${crypto.randomBytes(6).toString('hex')}.part`
                    );

                    console.log(
                        `[REMOTE CHUNK] ${latest.filename} | chunk ${index}/${latest.chunkCount - 1} | browser URL -> Node -> DingTalk`
                    );

                    await downloadWrappedChunkAndStripPrefix({
                        sourceUrl: url,
                        outputPath: localTemp,
                        expectedSize,
                        prefixBytes: REMOTE_CHUNK_PREFIX_BYTES
                    });

                    const stat = await fs.stat(localTemp);
                    if (stat.size !== expectedSize) {
                        throw new Error(`解包后的 chunk 大小错误：期望 ${expectedSize}，实际 ${stat.size}`);
                    }

                    const dingResult = await uploadChunkToDingTalk({
                        filePath: localTemp,
                        index
                    });

                    const saved = await withManifestLock(id, async () => {
                        const locked = await getManifest(id);
                        if (!locked) throw new Error('文件不存在');

                        if (locked.chunks[index]) {
                            const existing = locked.chunks[index];
                            return {
                                success: true,
                                index,
                                size: existing.size,
                                url: existing.url,
                                alreadyUploaded: true
                            };
                        }

                        locked.chunks[index] = {
                            index,
                            size: expectedSize,
                            url: dingResult.url,
                            uploadedAt: new Date().toISOString(),
                            source: 'browser-remote-url'
                        };

                        await saveManifest(locked);

                        return {
                            success: true,
                            index,
                            size: expectedSize,
                            url: dingResult.url,
                            alreadyUploaded: false
                        };
                    });

                    console.log(
                        `[REMOTE CHUNK OK] ${latest.filename} | chunk ${index}`
                    );
                    return saved;
                } finally {
                    if (localTemp) {
                        await fs.unlink(localTemp).catch(() => {});
                    }
                }
            })();

            tasks.set(taskKey, task);
            try {
                const result = await task;
                return res.json(result);
            } finally {
                if (tasks.get(taskKey) === task) {
                    tasks.delete(taskKey);
                }
            }
        } catch (error) {
            console.error('[REMOTE CHUNK ERROR]', error);
            if (tempFile) await fs.unlink(tempFile).catch(() => {});

            const message = error?.message || 'Internal Server Error';
            if (message === '文件不存在') {
                return res.status(404).json({ success: false, error: message });
            }
            if (
                message.includes('chunk index') ||
                message.includes('大小错误') ||
                message.includes('prefixBytes') ||
                message.includes('远程 URL 主机') ||
                message.includes('远程分片') ||
                message.includes('去除 125')
            ) {
                return res.status(400).json({ success: false, error: message });
            }
            return res.status(502).json({ success: false, error: message });
        }
    }
);

/*
|--------------------------------------------------------------------------
| Complete
|--------------------------------------------------------------------------
*/

app.post(
    '/api/videos/:id/complete',
    async (req, res) => {
        try {
            const result =
                await withManifestLock(
                    req.params.id,
                    async () => {
                        const manifest =
                            await getManifest(
                                req.params.id
                            );

                        if (!manifest) {
                            return {
                                status:
                                    404,

                                body: {
                                    success:
                                        false,
                                    error:
                                        '文件不存在'
                                }
                            };
                        }

                        const missing = [];
                        const invalid = [];

                        for (
                            let i = 0;
                            i <
                                manifest.chunkCount;
                            i++
                        ) {
                            const chunk =
                                manifest.chunks[
                                    i
                                ];

                            if (!chunk) {
                                missing.push(
                                    i
                                );
                                continue;
                            }

                            const expected =
                                Math.min(
                                    manifest.chunkSize,
                                    manifest.size -
                                        i *
                                            manifest.chunkSize
                                );

                            if (
                                chunk.size !==
                                expected
                            ) {
                                invalid.push({
                                    index:
                                        i,

                                    expected,

                                    actual:
                                        chunk.size
                                });
                            }
                        }

                        if (
                            missing.length ||
                            invalid.length
                        ) {
                            return {
                                status:
                                    400,

                                body: {
                                    success:
                                        false,

                                    error:
                                        '还有 chunk 未上传或 chunk 大小异常',

                                    missing,

                                    invalid
                                }
                            };
                        }

                        manifest.status =
                            'ready';

                        manifest.completedAt =
                            new Date()
                                .toISOString();

                        await saveManifest(
                            manifest
                        );

                        const fileUrl =
                            buildFileUrl(
                                manifest
                            );

                        console.log(
                            `[READY] ${manifest.filename} | ${manifest.size} bytes`
                        );

                        return {
                            status:
                                200,

                            body: {
                                success:
                                    true,

                                id:
                                    manifest.id,

                                filename:
                                    manifest.filename,

                                extension:
                                    manifest.extension,

                                contentType:
                                    manifest.contentType,

                                size:
                                    manifest.size,

                                chunkSize:
                                    manifest.chunkSize,

                                chunkCount:
                                    manifest.chunkCount,

                                fileUrl,

                                videoUrl:
                                    fileUrl,

                                legacyUrl:
                                    `/video/${manifest.id}`
                            }
                        };
                    }
                );

            return res
                .status(
                    result.status
                )
                .json(
                    result.body
                );
        } catch (error) {
            console.error(
                '[COMPLETE ERROR]',
                error
            );

            return res
                .status(500)
                .json({
                    success:
                        false,
                    error:
                        error.message ||
                        'Internal Server Error'
                });
        }
    }
);

/*
|--------------------------------------------------------------------------
| Parse /file/xxx.ext
|--------------------------------------------------------------------------
*/

async function getManifestFromFileRequest(
    req
) {
    let filename =
        req.path || '';

    filename =
        filename.replace(
            /^\/+/,
            ''
        );

    if (
        !filename ||
        filename.includes(
            '/'
        ) ||
        filename.includes(
            '\\'
        )
    ) {
        return null;
    }

    try {
        filename =
            decodeURIComponent(
                filename
            );
    } catch {
        return null;
    }

    let id = filename;

    const dot =
        filename.lastIndexOf(
            '.'
        );

    if (dot > 0) {
        id =
            filename.slice(
                0,
                dot
            );
    }

    if (!isValidId(id)) {
        return null;
    }

    return getManifest(
        id
    );
}

/*
|--------------------------------------------------------------------------
| File headers
|--------------------------------------------------------------------------
*/

function getFileHeaders(
    manifest
) {
    const filename =
        sanitizeFilename(
            manifest.filename
        );

    const encodedFilename =
        encodeURIComponent(
            filename
        );

    return {
        'Content-Type':
            manifest.contentType ||
            'application/octet-stream',

        'Accept-Ranges':
            'bytes',

        'Content-Disposition':
            `inline; filename*=UTF-8''${encodedFilename}`,

        'X-Content-Type-Options':
            'nosniff',

        /*
         * The Node shared chunk cache is the important cache.
         * Browser cache is kept short so seeking still behaves normally.
         */
        'Cache-Control':
            'public, max-age=60',

        'X-Video-Cache':
            'node-shared-chunk-cache'
    };
}

/*
|--------------------------------------------------------------------------
| Main File Handler
|--------------------------------------------------------------------------
*/

async function handleFileRequest(
    req,
    res,
    manifest
) {
    if (!manifest) {
        return res
            .status(404)
            .end();
    }

    if (
        manifest.status !==
        'ready'
    ) {
        return res
            .status(409)
            .json({
                success:
                    false,
                error:
                    'File is not ready'
            });
    }

    /*
     * HEAD
     */
    if (
        req.method ===
        'HEAD'
    ) {
        return res
            .status(200)
            .set({
                ...getFileHeaders(
                    manifest
                ),

                'Content-Length':
                    String(
                        manifest.size
                    )
            })
            .end();
    }

    const controller =
        new AbortController();

    let finished = false;

    const onClose = () => {
        if (
            !finished &&
            !res.writableEnded
        ) {
            console.log(
                `[CLIENT CLOSE] ${manifest.filename}`
            );

            try {
                controller.abort();
            } catch {}
        }
    };

    res.once(
        'close',
        onClose
    );

    try {
        let range;

        try {
            range =
                parseRange(
                    req.headers.range,
                    manifest.size
                );
        } catch (error) {
            console.warn(
                `[RANGE 416] ${manifest.filename} | ${req.headers.range || ''} | ${error.message}`
            );

            return res
                .status(416)
                .set(
                    'Content-Range',
                    `bytes */${manifest.size}`
                )
                .end();
        }

        /*
         * No Range:
         * stream the complete file through the same chunk cache.
         */
        if (!range) {
            res
                .status(200)
                .set({
                    ...getFileHeaders(
                        manifest
                    ),

                    'Content-Length':
                        String(
                            manifest.size
                        )
                });

            console.log(
                `[FULL] ${manifest.filename} | ${manifest.size} bytes`
            );

            await streamRangeCached({
                manifest,

                start:
                    0,

                end:
                    manifest.size - 1,

                response:
                    res,

                signal:
                    controller.signal
            });

            finished = true;

            return;
        }

        /*
         * Range response.
         */
        res
            .status(206)
            .set({
                ...getFileHeaders(
                    manifest
                ),

                'Content-Length':
                    String(
                        range.length
                    ),

                'Content-Range':
                    `bytes ${range.start}-${range.end}/${manifest.size}`
            });

        const firstChunk =
            Math.floor(
                range.start /
                    manifest.chunkSize
            );

        const lastChunk =
            Math.floor(
                range.end /
                    manifest.chunkSize
            );

        console.log(
            `[RANGE] ${manifest.filename} | ${range.start}-${range.end} | chunks=${firstChunk}-${lastChunk}`
        );

        await streamRangeCached({
            manifest,

            start:
                range.start,

            end:
                range.end,

            response:
                res,

            signal:
                controller.signal
        });

        finished = true;
    } catch (error) {
        if (
            controller.signal.aborted
        ) {
            return;
        }

        console.error(
            '[FILE ERROR]',
            error
        );

        if (
            !res.headersSent
        ) {
            return res
                .status(502)
                .json({
                    success:
                        false,
                    error:
                        error.message ||
                        'Upstream streaming error'
                });
        }

        if (!res.destroyed) {
            try {
                res.destroy();
            } catch {}
        }
    } finally {
        res.off(
            'close',
            onClose
        );
    }
}

/*
|--------------------------------------------------------------------------
| /file
|--------------------------------------------------------------------------
*/

app.use(
    '/file',
    async (
        req,
        res
    ) => {
        if (
            req.method !==
                'GET' &&
            req.method !==
                'HEAD'
        ) {
            res.set(
                'Allow',
                'GET, HEAD'
            );

            return res
                .status(405)
                .end();
        }

        const manifest =
            await getManifestFromFileRequest(
                req
            );

        return handleFileRequest(
            req,
            res,
            manifest
        );
    }
);

/*
|--------------------------------------------------------------------------
| Legacy /video/:id
|--------------------------------------------------------------------------
*/

app.get(
    '/video/:id',
    async (
        req,
        res
    ) => {
        const manifest =
            await getManifest(
                req.params.id
            );

        return handleFileRequest(
            req,
            res,
            manifest
        );
    }
);

app.head(
    '/video/:id',
    async (
        req,
        res
    ) => {
        const manifest =
            await getManifest(
                req.params.id
            );

        return handleFileRequest(
            req,
            res,
            manifest
        );
    }
);

/*
|--------------------------------------------------------------------------
| Cache status
|--------------------------------------------------------------------------
*/

app.get(
    '/api/cache',
    (
        req,
        res
    ) => {
        return res.json({
            success:
                true,

            limits: {
                memoryBytes:
                    MEMORY_CACHE_MAX_BYTES,

                memoryMiB:
                    MEMORY_CACHE_MAX_BYTES /
                    1024 /
                    1024,

                diskBytes:
                    DISK_CACHE_MAX_BYTES,

                diskGiB:
                    DISK_CACHE_MAX_BYTES /
                    1024 /
                    1024 /
                    1024,

                prefetchBytes:
                    PREFETCH_BYTES,

                prefetchMiB:
                    PREFETCH_BYTES /
                    1024 /
                    1024,

                prefetchChunks:
                    PREFETCH_CHUNKS,

                remoteConcurrency:
                    REMOTE_CONCURRENCY,

                prefetchConcurrency:
                    PREFETCH_CONCURRENCY
            },

            current: {
                memoryBytes:
                    memoryCacheBytes,

                memoryMiB:
                    memoryCacheBytes /
                    1024 /
                    1024,

                memoryEntries:
                    memoryCache.size,

                diskBytes:
                    diskCacheBytes,

                diskMiB:
                    diskCacheBytes /
                    1024 /
                    1024,

                diskEntries:
                    diskEntries.size,

                inflightChunks:
                    inflightChunks.size,

                prefetchJobs:
                    prefetchInFlight.size,

                remoteActive:
                    remoteSemaphore.active,

                remoteQueued:
                    remoteSemaphore.queued
            }
        });
    }
);

/*
|--------------------------------------------------------------------------
| Status
|--------------------------------------------------------------------------
*/

app.get(
    '/api/status',
    (
        req,
        res
    ) => {
        const memory =
            process.memoryUsage();

        return res.json({
            success:
                true,

            architecture: {
                upload:
                    'CHUNKED',

                uploadChunkSize:
                    CHUNK_SIZE,

                uploadConcurrency:
                    DEFAULT_UPLOAD_CONCURRENCY,

                uploadFakeMime:
                    'image/jpeg',

                uploadFakeExtension:
                    '.jpg',

                storage:
                    'MANIFEST + SHARED CHUNK CACHE',

                firstOpen:
                    'CACHE_FIRST + PREFETCH',

                range:
                    'CACHE_FIRST + RANGE_COALESCE',

                seek:
                    'CACHE_FIRST',

                sharedCache:
                    true,

                seekCache:
                    true,

                diskCache:
                    true,

                fullFileMemoryCache:
                    false,

                globalRemoteQueue:
                    true,

                globalRemoteConcurrencyLimit:
                    REMOTE_CONCURRENCY,

                remoteRange:
                    'FULL_CHUNK_206'
            },

            cache: {
                memoryLimitBytes:
                    MEMORY_CACHE_MAX_BYTES,

                memoryBytes:
                    memoryCacheBytes,

                memoryEntries:
                    memoryCache.size,

                diskLimitBytes:
                    DISK_CACHE_MAX_BYTES,

                diskBytes:
                    diskCacheBytes,

                diskEntries:
                    diskEntries.size,

                prefetchBytes:
                    PREFETCH_BYTES,

                prefetchChunks:
                    PREFETCH_CHUNKS,

                inflightChunks:
                    inflightChunks.size,

                prefetchJobs:
                    prefetchInFlight.size
            },

            chunkSize:
                CHUNK_SIZE,

            remoteTimeout:
                REMOTE_TIMEOUT_MS,

            remoteProxyMode:
                'SHARED_FULL_CHUNK_CACHE',

            memory: {
                rss:
                    memory.rss,

                heapUsed:
                    memory.heapUsed,

                heapTotal:
                    memory.heapTotal,

                external:
                    memory.external,

                arrayBuffers:
                    memory.arrayBuffers
            }
        });
    }
);

/*
|--------------------------------------------------------------------------
| Root
|--------------------------------------------------------------------------
*/

app.get(
    '/',
    (
        req,
        res
    ) => {
        return res.sendFile(
            path.resolve(
                './public/index.html'
            )
        );
    }
);

/*
|--------------------------------------------------------------------------
| Multer Error Handler
|--------------------------------------------------------------------------
*/

app.use(
    (
        error,
        req,
        res,
        next
    ) => {
        if (
            error instanceof
            multer.MulterError
        ) {
            if (
                error.code ===
                'LIMIT_FILE_SIZE'
            ) {
                return res
                    .status(413)
                    .json({
                        success:
                            false,

                        error:
                            `chunk 超过 ${CHUNK_SIZE} bytes`
                    });
            }

            return res
                .status(400)
                .json({
                    success:
                        false,

                    error:
                        error.message
                });
        }

        return next(
            error
        );
    }
);

/*
|--------------------------------------------------------------------------
| S3-compatible Gateway
|--------------------------------------------------------------------------
|
| This is an S3-compatible protocol facade only.
| The existing DingTalk-backed manifest/chunk/cache storage remains the
| actual backend. No S3 object storage is introduced.
|
| Supported:
|   Service: ListBuckets
|   Bucket:  HeadBucket, ListObjectsV2
|   Object:  GET, HEAD, PUT, DELETE
|   Range:   GET Range
|   Multipart: CreateMultipartUpload, UploadPart,
|              CompleteMultipartUpload, AbortMultipartUpload
|
| Authentication: AWS Signature Version 4 with one static access key.
|--------------------------------------------------------------------------
*/

const S3_BUCKET =
    process.env.S3_BUCKET || 'video';

const S3_ACCESS_KEY =
    process.env.S3_ACCESS_KEY || 'node2-access';

const S3_SECRET_KEY =
    process.env.S3_SECRET_KEY || 'change-this-secret';

const S3_REGION =
    process.env.S3_REGION || 'us-east-1';

const S3_SERVICE = 's3';

const S3_MULTIPART_DIR =
    path.join(STORAGE_DIR, 'uploads', '.s3-multipart');

await fs.mkdir(S3_MULTIPART_DIR, { recursive: true });

function s3XmlEscape(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
}

function s3Xml(res, status, body) {
    res.status(status);
    res.set('Content-Type', 'application/xml; charset=utf-8');
    return res.send(body);
}

function s3Error(res, code, message, status = 400, resource = '') {
    const requestId = crypto.randomBytes(16).toString('hex');
    const body =
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<Error>` +
        `<Code>${s3XmlEscape(code)}</Code>` +
        `<Message>${s3XmlEscape(message)}</Message>` +
        `<Resource>${s3XmlEscape(resource)}</Resource>` +
        `<RequestId>${requestId}</RequestId>` +
        `</Error>`;

    res.set('x-amz-request-id', requestId);
    res.set('x-amz-id-2', requestId);
    return s3Xml(res, status, body);
}

function s3EncodeKey(key) {
    return encodeURIComponent(key)
        .replace(/%2F/gi, '/')
        .replace(/%2B/gi, '%2B');
}

function s3ObjectUrl(key) {
    return `/s3/${S3_BUCKET}/${s3EncodeKey(key)}`;
}

function s3ManifestKey(manifest) {
    if (
        typeof manifest.s3Key === 'string' &&
        manifest.s3Key.trim()
    ) {
        return manifest.s3Key.trim().replace(/^\/+/, '');
    }

    return `${manifest.id}${manifest.extension || ''}`;
}

function s3NormalizeKey(key) {
    try {
        key = decodeURIComponent(key);
    } catch {}

    key = String(key || '').replace(/^\/+/, '');

    if (!key || key.length > 1024) {
        return null;
    }

    if (key.includes('\0')) {
        return null;
    }

    return key;
}

async function s3ListManifests() {
    let names = [];

    try {
        names = await fs.readdir(MANIFEST_DIR);
    } catch {
        return [];
    }

    const results = [];

    for (const name of names) {
        if (!name.endsWith('.json')) continue;

        try {
            const text = await fs.readFile(
                path.join(MANIFEST_DIR, name),
                'utf8'
            );
            const manifest = JSON.parse(text);

            if (!manifest?.id) continue;
            results.push(manifest);
        } catch {}
    }

    return results;
}

async function s3FindManifest(key) {
    const manifests = await s3ListManifests();

    for (const manifest of manifests) {
        if (s3ManifestKey(manifest) === key) {
            return manifest;
        }
    }

    return null;
}

function s3ObjectEtag(manifest) {
    if (manifest.s3Etag) return manifest.s3Etag;

    const hash = crypto.createHash('md5');
    hash.update(`${manifest.id}:${manifest.size}:${manifest.createdAt || ''}`);
    return hash.digest('hex');
}

function s3Date(date = new Date()) {
    return date.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
}

function s3AmzDate(date = new Date()) {
    return s3Date(date);
}

function s3Hash(value) {
    return crypto.createHash('sha256').update(value).digest('hex');
}

function s3Hmac(key, value) {
    return crypto.createHmac('sha256', key).update(value).digest();
}

function s3HmacHex(key, value) {
    return crypto.createHmac('sha256', key).update(value).digest('hex');
}

function s3SigningKey(secret, date, region, service) {
    const kDate = s3Hmac(`AWS4${secret}`, date);
    const kRegion = s3Hmac(kDate, region);
    const kService = s3Hmac(kRegion, service);
    return s3Hmac(kService, 'aws4_request');
}

function s3CanonicalUri(req) {
    const raw = req.originalUrl.split('?')[0] || '/';
    const parts = raw.split('/');
    return parts.map((part, index) => {
        if (index === 0) return '';
        try {
            return encodeURIComponent(decodeURIComponent(part));
        } catch {
            return encodeURIComponent(part);
        }
    }).join('/') || '/';
}

function s3CanonicalQuery(req) {
    const pairs = [];

    for (const [key, value] of new URL(req.protocol + '://' + (req.get('host') || 'localhost') + req.originalUrl).searchParams.entries()) {
        pairs.push([
            encodeURIComponent(key),
            encodeURIComponent(value)
        ]);
    }

    pairs.sort((a, b) => {
        if (a[0] === b[0]) return a[1].localeCompare(b[1]);
        return a[0].localeCompare(b[0]);
    });

    return pairs.map(([k, v]) => `${k}=${v}`).join('&');
}

function s3CanonicalHeaders(req, signedHeaders) {
    const names = signedHeaders.split(';').filter(Boolean).sort();
    const lines = [];

    for (const name of names) {
        const value = req.headers[name];
        if (value === undefined) {
            throw new Error(`Missing signed header: ${name}`);
        }

        const normalized = Array.isArray(value)
            ? value.join(',')
            : String(value).trim().replace(/\s+/g, ' ');

        lines.push(`${name}:${normalized}`);
    }

    return {
        names: names.join(';'),
        value: lines.join('\n') + '\n'
    };
}

function s3AuthError(res, req, message = 'Access Denied') {
    return s3Error(
        res,
        'AccessDenied',
        message,
        403,
        req.originalUrl
    );
}

function s3VerifySignature(req, rawBodyHash = null) {
    const authorization = req.headers.authorization;
    const amzDate = req.headers['x-amz-date'];

    if (!authorization || !amzDate) {
        return { ok: false, message: 'Missing AWS Signature V4 headers' };
    }

    const match = authorization.match(
        /^AWS4-HMAC-SHA256 Credential=([^/]+)\/(\d{8})\/([^/]+)\/([^/]+)\/aws4_request, SignedHeaders=([^,]+), Signature=([0-9a-f]{64})$/i
    );

    if (!match) {
        return { ok: false, message: 'Invalid Authorization header' };
    }

    const [, accessKey, dateStamp, region, service, signedHeaders, signature] = match;

    if (accessKey !== S3_ACCESS_KEY) {
        return { ok: false, message: 'Invalid access key' };
    }

    if (region !== S3_REGION || service !== S3_SERVICE) {
        return { ok: false, message: 'Invalid credential scope' };
    }

    const canonical = s3CanonicalHeaders(req, signedHeaders);
    const payloadHash =
        rawBodyHash ||
        req.headers['x-amz-content-sha256'] ||
        'UNSIGNED-PAYLOAD';

    const canonicalRequest = [
        req.method,
        s3CanonicalUri(req),
        s3CanonicalQuery(req),
        canonical.value,
        canonical.names,
        payloadHash
    ].join('\n');

    const hashedCanonicalRequest =
        s3Hash(canonicalRequest);

    const stringToSign = [
        'AWS4-HMAC-SHA256',
        amzDate,
        `${dateStamp}/${region}/${service}/aws4_request`,
        hashedCanonicalRequest
    ].join('\n');

    const signingKey = s3SigningKey(
        S3_SECRET_KEY,
        dateStamp,
        region,
        service
    );

    const expected = s3HmacHex(
        signingKey,
        stringToSign
    );

    if (
        !crypto.timingSafeEqual(
            Buffer.from(expected),
            Buffer.from(signature)
        )
    ) {
        return { ok: false, message: 'Signature mismatch' };
    }

    return { ok: true };
}

async function s3ReadRequestBodyToFile(req, filePath) {
    return new Promise((resolve, reject) => {
        let total = 0;
        const out = requireUnavailableFsCreateWriteStream(filePath);

        req.on('data', chunk => {
            total += chunk.length;
        });

        req.on('error', reject);
        out.on('error', reject);
        out.on('finish', () => resolve(total));

        req.pipe(out);
    });
}

/* ESM-safe lazy createWriteStream helper. */
function requireUnavailableFsCreateWriteStream(filePath) {
    // fs/promises is intentionally used everywhere else in this file.
    // A synchronous require is unavailable in ESM, so use the imported
    // promise API through a tiny temporary bridge created below.
    return fsStreamCreateWriteStream(filePath);
}

/*
 * Node's fs/promises module intentionally has no createWriteStream.
 * The function is replaced at startup by the built-in node:fs stream API.
 */
import { createWriteStream as fsCreateWriteStream } from 'node:fs';
function fsStreamCreateWriteStream(filePath) {
    return fsCreateWriteStream(filePath);
}

async function s3SplitAndUploadFile({ filePath, manifest }) {
    const stat = await fs.stat(filePath);
    const totalSize = stat.size;

    if (totalSize !== manifest.size) {
        throw new Error(`S3 body size mismatch: expected ${manifest.size}, got ${totalSize}`);
    }

    const fd = await fs.open(filePath, 'r');
    const tempParts = [];

    try {
        for (let index = 0; index < manifest.chunkCount; index++) {
            const start = index * CHUNK_SIZE;
            const length = Math.min(CHUNK_SIZE, totalSize - start);
            const partPath = path.join(
                UPLOAD_DIR,
                `.s3-${manifest.id}-${index}-${crypto.randomBytes(4).toString('hex')}.part`
            );

            const buffer = Buffer.allocUnsafe(length);
            let offset = 0;

            while (offset < length) {
                const { bytesRead } = await fd.read(
                    buffer,
                    offset,
                    length - offset,
                    start + offset
                );

                if (!bytesRead) {
                    throw new Error(`Unexpected EOF at chunk ${index}`);
                }

                offset += bytesRead;
            }

            await fs.writeFile(partPath, buffer);
            tempParts.push(partPath);

            const result = await uploadChunkToDingTalk({
                filePath: partPath,
                index
            });

            manifest.chunks[index] = {
                index,
                size: length,
                url: result.url,
                uploadedAt: new Date().toISOString()
            };

            await fs.unlink(partPath).catch(() => {});
        }

        manifest.status = 'ready';
        manifest.completedAt = new Date().toISOString();
        await saveManifest(manifest);
    } finally {
        await fd.close().catch(() => {});
        for (const partPath of tempParts) {
            await fs.unlink(partPath).catch(() => {});
        }
    }
}

function s3KeyFromRequest(req) {
    const rawPath = req.path || '';
    const key = rawPath.replace(/^\/+/, '');
    return s3NormalizeKey(key);
}

async function s3GetObject(manifest, req, res) {
    return handleFileRequest(req, res, manifest);
}

async function s3CreateMultipart(req, res, key) {
    const uploadId = crypto.randomUUID();
    const dir = path.join(S3_MULTIPART_DIR, uploadId);
    await fs.mkdir(dir, { recursive: true });

    const meta = {
        uploadId,
        bucket: S3_BUCKET,
        key,
        filename: path.basename(key) || 'file',
        contentType: req.headers['content-type'] || 'application/octet-stream',
        createdAt: new Date().toISOString(),
        parts: {}
    };

    await fs.writeFile(
        path.join(dir, 'meta.json'),
        JSON.stringify(meta, null, 2),
        'utf8'
    );

    return s3Xml(
        res,
        200,
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<InitiateMultipartUploadResult>` +
        `<Bucket>${s3XmlEscape(S3_BUCKET)}</Bucket>` +
        `<Key>${s3XmlEscape(key)}</Key>` +
        `<UploadId>${s3XmlEscape(uploadId)}</UploadId>` +
        `</InitiateMultipartUploadResult>`
    );
}

async function s3LoadMultipart(uploadId) {
    if (!/^[a-zA-Z0-9-]+$/.test(uploadId)) return null;

    try {
        const dir = path.join(S3_MULTIPART_DIR, uploadId);
        const meta = JSON.parse(
            await fs.readFile(
                path.join(dir, 'meta.json'),
                'utf8'
            )
        );

        return { dir, meta };
    } catch {
        return null;
    }
}

async function s3UploadPart(req, res, key) {
    const uploadId = String(req.query.uploadId || '');
    const partNumber = Number(req.query.partNumber || 0);
    const multipart = await s3LoadMultipart(uploadId);

    if (!multipart || multipart.meta.key !== key) {
        return s3Error(res, 'NoSuchUpload', 'The specified upload does not exist', 404);
    }

    if (!Number.isInteger(partNumber) || partNumber < 1 || partNumber > 10000) {
        return s3Error(res, 'InvalidPart', 'Invalid part number', 400);
    }

    const partPath = path.join(
        multipart.dir,
        `part-${String(partNumber).padStart(5, '0')}.bin`
    );

    const size = await s3ReadRequestBodyToFile(req, partPath);

    if (size <= 0 || size > CHUNK_SIZE) {
        await fs.unlink(partPath).catch(() => {});
        return s3Error(res, 'EntityTooLarge', `Part must be between 1 byte and ${CHUNK_SIZE} bytes`, 400);
    }

    const etag = crypto.createHash('md5')
        .update(await fs.readFile(partPath))
        .digest('hex');

    multipart.meta.parts[String(partNumber)] = {
        partNumber,
        size,
        etag
    };

    await fs.writeFile(
        path.join(multipart.dir, 'meta.json'),
        JSON.stringify(multipart.meta, null, 2),
        'utf8'
    );

    res.set('ETag', `"${etag}"`);
    return res.status(200).end();
}

async function s3CompleteMultipart(req, res, key) {
    const uploadId = String(req.query.uploadId || '');
    const multipart = await s3LoadMultipart(uploadId);

    if (!multipart || multipart.meta.key !== key) {
        return s3Error(res, 'NoSuchUpload', 'The specified upload does not exist', 404);
    }

    let xml = '';
    try {
        xml = await new Promise((resolve, reject) => {
            let text = '';
            req.setEncoding('utf8');
            req.on('data', chunk => text += chunk);
            req.on('end', () => resolve(text));
            req.on('error', reject);
        });
    } catch {
        return s3Error(res, 'MalformedXML', 'Could not read CompleteMultipartUpload request', 400);
    }

    const requestedParts = [...xml.matchAll(/<Part>\s*<PartNumber>(\d+)<\/PartNumber>\s*<ETag>"?([^<"]+)"?<\/ETag>\s*<\/Part>/g)]
        .map(m => ({ partNumber: Number(m[1]), etag: m[2] }))
        .sort((a, b) => a.partNumber - b.partNumber);

    if (!requestedParts.length) {
        return s3Error(res, 'MalformedXML', 'No multipart parts supplied', 400);
    }

    for (let i = 0; i < requestedParts.length; i++) {
        const part = requestedParts[i];
        const stored = multipart.meta.parts[String(part.partNumber)];

        if (!stored || stored.etag !== part.etag) {
            return s3Error(res, 'InvalidPart', `Part ${part.partNumber} is invalid`, 400);
        }

        if (part.partNumber !== i + 1) {
            return s3Error(res, 'InvalidPartOrder', 'Parts must be in ascending order', 400);
        }
    }

    const id = crypto.randomUUID();
    const size = requestedParts.reduce(
        (sum, part) => sum + multipart.meta.parts[String(part.partNumber)].size,
        0
    );

    const manifest = createManifest({
        id,
        filename: multipart.meta.filename,
        size,
        contentType: multipart.meta.contentType,
        s3Key: key
    });

    try {
        for (const part of requestedParts) {
            const stored = multipart.meta.parts[String(part.partNumber)];
            const partPath = path.join(
                multipart.dir,
                `part-${String(part.partNumber).padStart(5, '0')}.bin`
            );

            const result = await uploadChunkToDingTalk({
                filePath: partPath,
                index: part.partNumber - 1
            });

            manifest.chunks[part.partNumber - 1] = {
                index: part.partNumber - 1,
                size: stored.size,
                url: result.url,
                uploadedAt: new Date().toISOString()
            };
        }

        manifest.status = 'ready';
        manifest.completedAt = new Date().toISOString();
        await saveManifest(manifest);
    } finally {
        await fs.rm(multipart.dir, { recursive: true, force: true }).catch(() => {});
    }

    const etag = s3ObjectEtag(manifest);

    return s3Xml(
        res,
        200,
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<CompleteMultipartUploadResult>` +
        `<Location>${s3XmlEscape(s3ObjectUrl(key))}</Location>` +
        `<Bucket>${s3XmlEscape(S3_BUCKET)}</Bucket>` +
        `<Key>${s3XmlEscape(key)}</Key>` +
        `<ETag>"${s3XmlEscape(etag)}"</ETag>` +
        `</CompleteMultipartUploadResult>`
    );
}

async function s3AbortMultipart(req, res) {
    const uploadId = String(req.query.uploadId || '');
    const multipart = await s3LoadMultipart(uploadId);

    if (!multipart) {
        return s3Error(res, 'NoSuchUpload', 'The specified upload does not exist', 404);
    }

    await fs.rm(multipart.dir, { recursive: true, force: true });
    return res.status(204).end();
}

async function s3PutObject(req, res, key) {
    const existing = await s3FindManifest(key);
    if (existing) {
        return s3Error(res, 'EntityAlreadyExists', 'Object already exists', 409, key);
    }

    const tempPath = path.join(
        UPLOAD_DIR,
        `.s3-put-${crypto.randomUUID()}.bin`
    );

    try {
        const size = await s3ReadRequestBodyToFile(req, tempPath);

        if (size <= 0) {
            return s3Error(res, 'InvalidRequest', 'Empty object is not supported', 400, key);
        }

        const filename = path.basename(key) || 'file';
        const contentType =
            req.headers['content-type'] ||
            getMimeType(filename, 'application/octet-stream');

        const manifest = createManifest({
            id: crypto.randomUUID(),
            filename,
            size,
            contentType,
            s3Key: key
        });

        await s3SplitAndUploadFile({
            filePath: tempPath,
            manifest
        });

        const etag = s3ObjectEtag(manifest);
        res.set('ETag', `"${etag}"`);
        return res.status(200).end();
    } catch (error) {
        console.error('[S3 PUT ERROR]', error);
        return s3Error(res, 'InternalError', error.message || 'Upload failed', 500, key);
    } finally {
        await fs.unlink(tempPath).catch(() => {});
    }
}

async function s3ListObjects(req, res) {
    const prefix = String(req.query.prefix || '');
    const delimiter = String(req.query.delimiter || '');
    const maxKeys = Math.min(1000, Math.max(1, Number(req.query['max-keys'] || 1000)));
    const startAfter = String(req.query.startAfter || '');
    const continuation = String(req.query.continuationToken || '');

    const manifests = await s3ListManifests();
    const objects = manifests
        .filter(m => m.status === 'ready')
        .map(m => ({
            manifest: m,
            key: s3ManifestKey(m)
        }))
        .filter(o => o.key.startsWith(prefix))
        .sort((a, b) => a.key.localeCompare(b.key));

    let filtered = objects;

    if (startAfter) {
        filtered = filtered.filter(o => o.key > startAfter);
    }

    if (continuation) {
        try {
            const decoded = Buffer.from(continuation, 'base64url').toString('utf8');
            filtered = filtered.filter(o => o.key > decoded);
        } catch {}
    }

    const selected = filtered.slice(0, maxKeys);
    const truncated = filtered.length > selected.length;
    const nextToken = truncated
        ? Buffer.from(selected[selected.length - 1].key).toString('base64url')
        : '';

    const commonPrefixes = new Set();
    const contents = [];

    for (const item of selected) {
        if (delimiter) {
            const rest = item.key.slice(prefix.length);
            const slash = rest.indexOf(delimiter);

            if (slash >= 0) {
                commonPrefixes.add(
                    prefix + rest.slice(0, slash + delimiter.length)
                );
                continue;
            }
        }

        contents.push(
            `<Contents>` +
            `<Key>${s3XmlEscape(item.key)}</Key>` +
            `<LastModified>${s3XmlEscape(item.manifest.completedAt || item.manifest.createdAt)}</LastModified>` +
            `<ETag>"${s3XmlEscape(s3ObjectEtag(item.manifest))}"</ETag>` +
            `<Size>${item.manifest.size}</Size>` +
            `<StorageClass>STANDARD</StorageClass>` +
            `</Contents>`
        );
    }

    const prefixes = [...commonPrefixes]
        .sort()
        .map(p => `<CommonPrefixes><Prefix>${s3XmlEscape(p)}</Prefix></CommonPrefixes>`)
        .join('');

    return s3Xml(
        res,
        200,
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<ListBucketResult>` +
        `<Name>${s3XmlEscape(S3_BUCKET)}</Name>` +
        `<Prefix>${s3XmlEscape(prefix)}</Prefix>` +
        `<KeyCount>${contents.length}</KeyCount>` +
        `<MaxKeys>${maxKeys}</MaxKeys>` +
        `<IsTruncated>${truncated}</IsTruncated>` +
        (continuation ? `<ContinuationToken>${s3XmlEscape(continuation)}</ContinuationToken>` : '') +
        (nextToken ? `<NextContinuationToken>${s3XmlEscape(nextToken)}</NextContinuationToken>` : '') +
        contents.join('') +
        prefixes +
        `</ListBucketResult>`
    );
}

async function s3ListBuckets(req, res) {
    return s3Xml(
        res,
        200,
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<ListAllMyBucketsResult>` +
        `<Owner><ID>node2</ID><DisplayName>node2</DisplayName></Owner>` +
        `<Buckets><Bucket>` +
        `<Name>${s3XmlEscape(S3_BUCKET)}</Name>` +
        `<CreationDate>1970-01-01T00:00:00.000Z</CreationDate>` +
        `</Bucket></Buckets>` +
        `</ListAllMyBucketsResult>`
    );
}

async function s3Handle(req, res) {
    const auth = s3VerifySignature(req);

    if (!auth.ok) {
        return s3AuthError(res, req, auth.message);
    }

    const parts = req.path
        .split('/')
        .filter(Boolean);

    const bucket = parts[0] || '';
    const key = s3NormalizeKey(parts.slice(1).join('/'));

    if (!bucket) {
        if (req.method === 'GET') {
            return s3ListBuckets(req, res);
        }
        return s3Error(res, 'InvalidRequest', 'Invalid S3 request', 400);
    }

    if (bucket !== S3_BUCKET) {
        return s3Error(res, 'NoSuchBucket', 'The specified bucket does not exist', 404, bucket);
    }

    const hasUploadId = !!req.query.uploadId;
    const hasListType2 = req.query['list-type'] === '2';

    if (!key && req.method === 'GET' && hasListType2) {
        return s3ListObjects(req, res);
    }

    if (!key && req.method === 'GET' && !hasUploadId) {
        return s3ListObjects(req, res);
    }

    if (!key) {
        if (req.method === 'HEAD') return res.status(200).end();
        return s3Error(res, 'InvalidRequest', 'Bucket request is invalid', 400);
    }

    if (hasUploadId) {
        if (req.method === 'POST' && !('partNumber' in req.query)) {
            return s3CompleteMultipart(req, res, key);
        }

        if (req.method === 'PUT' && 'partNumber' in req.query) {
            return s3UploadPart(req, res, key);
        }

        if (req.method === 'DELETE') {
            return s3AbortMultipart(req, res);
        }

        return s3Error(res, 'InvalidRequest', 'Invalid multipart request', 400, key);
    }

    if (req.method === 'POST' && req.query.uploads !== undefined) {
        return s3CreateMultipart(req, res, key);
    }

    if (req.method === 'HEAD') {
        const manifest = await s3FindManifest(key);
        if (!manifest) {
            return s3Error(res, 'NoSuchKey', 'The specified key does not exist', 404, key);
        }

        res.set({
            'Content-Type': manifest.contentType || 'application/octet-stream',
            'Content-Length': String(manifest.size),
            'Accept-Ranges': 'bytes',
            'ETag': `"${s3ObjectEtag(manifest)}"`,
            'Last-Modified': new Date(manifest.completedAt || manifest.createdAt || Date.now()).toUTCString()
        });

        return res.status(200).end();
    }

    if (req.method === 'GET') {
        const manifest = await s3FindManifest(key);
        if (!manifest) {
            return s3Error(res, 'NoSuchKey', 'The specified key does not exist', 404, key);
        }

        res.set('ETag', `"${s3ObjectEtag(manifest)}"`);
        res.set('Last-Modified', new Date(manifest.completedAt || manifest.createdAt || Date.now()).toUTCString());
        return s3GetObject(manifest, req, res);
    }

    if (req.method === 'PUT') {
        return s3PutObject(req, res, key);
    }

    if (req.method === 'DELETE') {
        const manifest = await s3FindManifest(key);
        if (!manifest) return res.status(204).end();

        /* Keep the existing manifest/chunk storage model. */
        manifest.status = 'deleted';
        manifest.deletedAt = new Date().toISOString();
        await saveManifest(manifest);

        /* Drop related playback cache. */
        for (const cacheIndex of Object.keys(manifest.chunks || {})) {
            const cacheKeyValue = cacheKey(manifest.id, Number(cacheIndex));
            const mem = memoryCache.get(cacheKeyValue);
            if (mem) {
                memoryCache.delete(cacheKeyValue);
                memoryCacheBytes -= mem.buffer.length;
            }
            const disk = diskEntries.get(cacheKeyValue);
            if (disk) {
                await removeDiskEntry(cacheKeyValue, disk);
            }
        }

        return res.status(204).end();
    }

    res.set('Allow', 'GET, HEAD, PUT, DELETE, POST');
    return s3Error(res, 'MethodNotAllowed', 'Method not allowed', 405, key);
}

/*
 * S3 API endpoint.
 *
 * Example:
 *   S3 endpoint = http://host:8020/s3
 *   bucket      = video
 *   key         = movies/a.mp4
 */
app.use('/s3', async (req, res) => {
    try {
        return await s3Handle(req, res);
    } catch (error) {
        console.error('[S3 GATEWAY ERROR]', error);
        if (res.headersSent) return res.end();
        return s3Error(res, 'InternalError', error.message || 'Internal Server Error', 500, req.originalUrl);
    }
});


/*
|--------------------------------------------------------------------------
| 404
|--------------------------------------------------------------------------
*/

app.use(
    (
        req,
        res
    ) => {
        return res
            .status(404)
            .json({
                success:
                    false,
                error:
                    'Not Found'
            });
    }
);

/*
|--------------------------------------------------------------------------
| Global Error
|--------------------------------------------------------------------------
*/

app.use(
    (
        error,
        req,
        res,
        next
    ) => {
        console.error(
            '[GLOBAL ERROR]',
            error
        );

        if (
            res.headersSent
        ) {
            return next(
                error
            );
        }

        return res
            .status(500)
            .json({
                success:
                    false,

                error:
                    error.message ||
                    'Internal Server Error'
            });
    }
);

/*
|--------------------------------------------------------------------------
| Start
|--------------------------------------------------------------------------
*/

await initDiskCache();

const server =
    app.listen(
        PORT,
        () => {
            console.log('');
            console.log(
                '=========================================='
            );
            console.log(
                ' Universal File Streaming Server'
            );
            console.log(
                ' 5MB Chunk + Shared Cache'
            );
            console.log(
                ' DingTalk Fake JPG Upload'
            );
            console.log(
                ' 64MiB Memory LRU + 1GiB Disk Cache'
            );
            console.log(
                ' 50MiB Background Prefetch'
            );
            console.log(
                ' Shared In-flight Range/Chunk Merge'
            );
            console.log(
                '=========================================='
            );
            console.log(
                `Server: http://0.0.0.0:${PORT}`
            );
            console.log(
                `Node: ${process.version}`
            );
            console.log(
                `Chunk Size: ${CHUNK_SIZE} bytes (5 MiB)`
            );
            console.log(
                `Upload Concurrency: ${DEFAULT_UPLOAD_CONCURRENCY}`
            );
            console.log(
                `Remote Timeout: ${REMOTE_TIMEOUT_MS} ms`
            );
            console.log(
                `Remote Concurrency: ${REMOTE_CONCURRENCY}`
            );
            console.log(
                `Memory Cache: ${formatMiB(MEMORY_CACHE_MAX_BYTES)}`
            );
            console.log(
                `Disk Cache: ${formatMiB(DISK_CACHE_MAX_BYTES)}`
            );
            console.log(
                `Prefetch: ${formatMiB(PREFETCH_BYTES)} (${PREFETCH_CHUNKS} chunks)`
            );
            console.log(
                'Shared Cache: ON'
            );
            console.log(
                'Seek Cache: ON'
            );
            console.log(
                'Disk Cache: ON'
            );
            console.log(
                'Prefetch: ON'
            );
            console.log(
                '=========================================='
            );
            console.log('');
        }
    );

/*
|--------------------------------------------------------------------------
| HTTP server tuning
|--------------------------------------------------------------------------
|
| Do not create a giant in-memory response buffer.
| The actual video cache is controlled above.
|--------------------------------------------------------------------------
*/

server.requestTimeout = 0;

server.headersTimeout =
    Math.max(
        30000,
        REMOTE_TIMEOUT_MS
    );

server.keepAliveTimeout =
    65000;
