# Go 360 + DingTalk file server

This package removes Telegram and S3/R2 integrations. It keeps the Go server, web frontend, local cache, file streaming/Range support, 360 upload, and DingTalk fallback.

## Run

```sh
cp .env.example .env
chmod +x server
./server
```

Default listen port: `8023` (`PORT` overrides it). Configure the DingTalk backup credentials in `.env` if fallback is required.

## 360 upload behavior

- The browser's normal chunk upload endpoint now uploads each chunk to 360 first.
- It requests `GET https://console.e.360.cn/api/v1/UploadToken`, reads JSON `data`, then posts multipart fields `token`, `picasso=1`, and `uploadfile` to `https://up2.e.360.cn/up`.
- `uploadfile` is sent as `image/png` with a 125-byte prefix followed by the original chunk bytes.
- Successful upload stores `data.app_url` as the primary URL. The original chunk is also retained locally for recovery.
- If the 360 upload itself fails, the code attempts DingTalk fallback and logs the error details.
- Remote range reads from a 360 `qhimg.com` URL add 125 to the requested byte offsets to skip the prefix.

## Link upload

`POST /api/upload-url` with JSON `{"url":"https://example.com/video.mp4"}` uploads a remote media URL to 360. For ordinary files, the source response is streamed in 5 MiB pieces; at most eight pieces are in flight and uploaded concurrently. The whole source is not first downloaded to memory or staged as a complete local file. Each piece buffer is released after its upload completes. The JSON response returns ordered `parts` with source offsets, sizes, and 360 URLs. This response describes the pieces; a separate concatenation/virtual-file endpoint is required to expose them as one playable file URL. M3U8 inputs are detected and their referenced segments are uploaded separately; live playlist polling and rewritten playlist publishing are not implemented by this endpoint.

## Diagnostics

Server logs include `[360 UPLOAD OK]` on successful primary uploads and `[360 UPLOAD FAILED]` with the reason before DingTalk fallback. Token, HTTP status, `errno`, and `app_url` errors are returned from the 360 helper.

The binary was built and startup was checked in the build environment. A real 360 account/network upload could not be live-verified from that environment, so test an actual upload on your server and inspect the `[360 UPLOAD OK]` log.

## 360 PNG-prefix fix

The 360 upload path now creates a valid 125-byte 1x1 PNG prefix (PNG signature, valid IHDR/IDAT, a CRC-correct tEXt chunk, and IEND), then appends the original chunk bytes unchanged. Each uploaded object uses a random `.png` filename. Remote reads compensate for the 125-byte prefix when constructing Range requests. This fixes the prior invalid-header construction that could trigger `errno=1 创建图片失败`.

The build was compiled locally and the 125-byte prefix was unit-tested with Go's PNG decoder. A live 360 account upload still must be confirmed from the deployment server.

## 360 链接上传（修订版）

- 普通文件以 5 MiB 分片、最多 8 并发上传到 360；后端把任务清单写入 `storage/linkjobs/asset-<id>.json`，成功的分片 URL、偏移量和大小会被保存。
- 普通文件返回的 `url` 是本服务器 `/linkasset/<id>`，不是某一个 360 图片 URL。该接口读取每个 360 对象时剥离已验证的 125 字节 PNG 前缀，并提供 Range 响应，用于按原始字节读取分片。
- 上传失败时会保存已经成功上传的分片清单，并返回 `uploadedParts`、任务 ID 和错误；目前这表示可诊断/恢复所需的数据已持久化，但自动续传远程源下载还需要源站支持 Range/稳定 ETag，程序不会伪称已经自动续传。
- M3U8 会清理 BOM、CRLF 和 NUL 字符；遇到 `#EXT-X-STREAM-INF` 会递归读取对应的子播放列表，媒体播放列表中的媒体 URI 会上传到 360，并生成本服务器的重写播放列表 `/linkplaylist/<id>.m3u8`。
- 目前直播列表没有后台定时轮询刷新；含有加密密钥或 `EXT-X-MAP` URI 属性的特殊 HLS 清单也需要单独重写这些 URI。对这些场景接口会受源清单格式限制，不应视为全量 HLS 兼容。
