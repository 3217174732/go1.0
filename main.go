package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"container/list"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	"image/png"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed public/index.html
var publicIndex []byte

const (
	defaultPort                = "8023"
	chunkSize                  = int64(5 * 1024 * 1024)   // 5 MiB
	memoryCacheMax             = int64(512 * 1024 * 1024) // 512 MiB
	diskCacheMax               = int64(20 * 1024 * 1024 * 1024)
	prefetchBytes              = int64(128 * 1024 * 1024)
	defaultRemoteConcurrency   = 8
	defaultPrefetchConcurrency = 4
	defaultUploadConcurrency   = 8
	remoteTimeoutDefault       = 120 * time.Second
	retryCountDefault          = 5
	retryBaseDelayDefault      = 1500 * time.Millisecond
)

type Config struct {
	Port                 string
	RemoteTimeout        time.Duration
	RemoteRetryCount     int
	RemoteRetryBaseDelay time.Duration
	RemoteConcurrency    int
	PrefetchConcurrency  int
	UploadConcurrency    int
	DingTalkUploadURL    string
	DingTalkCookie       string
	DingTalkToken        string
	StorageDir           string
}

func envString(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return d
}
func envDurationMS(k string, d time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1000 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return d
}
func loadDotEnv() {
	b, err := os.ReadFile(".env")
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.IndexByte(line, '=')
		if p <= 0 {
			continue
		}
		k := strings.TrimSpace(line[:p])
		v := strings.TrimSpace(line[p+1:])
		v = strings.Trim(v, "\"")
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
}

func loadConfig() Config {
	return Config{
		Port:                 envString("PORT", defaultPort),
		RemoteTimeout:        envDurationMS("REMOTE_TIMEOUT_MS", remoteTimeoutDefault),
		RemoteRetryCount:     envInt("REMOTE_RETRY_COUNT", retryCountDefault),
		RemoteRetryBaseDelay: time.Duration(envInt("REMOTE_RETRY_BASE_DELAY_MS", int(retryBaseDelayDefault/time.Millisecond))) * time.Millisecond,
		RemoteConcurrency:    envInt("REMOTE_CONCURRENCY", defaultRemoteConcurrency),
		PrefetchConcurrency:  envInt("PREFETCH_CONCURRENCY", defaultPrefetchConcurrency),
		UploadConcurrency:    envInt("DINGTALK_UPLOAD_CONCURRENCY", defaultUploadConcurrency),
		DingTalkUploadURL:    envString("DINGTALK_UPLOAD_URL", "https://h5.dingtalk.com/common/picUpload"),
		DingTalkCookie:       os.Getenv("DINGTALK_COOKIE"),
		DingTalkToken:        os.Getenv("DINGTALK_TOKEN"),
		StorageDir:           envString("STORAGE_DIR", "./storage"),
	}
}

type ChunkInfo struct {
	Index       int    `json:"index"`
	Size        int64  `json:"size"`
	URL         string `json:"url"`
	UploadedAt  string `json:"uploadedAt"`
	Source      string `json:"source,omitempty"`
	DingTalkURL string `json:"dingTalkUrl,omitempty"`
}

type Manifest struct {
	ID           string               `json:"id"`
	Filename     string               `json:"filename"`
	Extension    string               `json:"extension"`
	ContentType  string               `json:"contentType"`
	Size         int64                `json:"size"`
	ChunkSize    int64                `json:"chunkSize"`
	ChunkCount   int                  `json:"chunkCount"`
	Chunks       map[string]ChunkInfo `json:"chunks"`
	Status       string               `json:"status"`
	CreatedAt    string               `json:"createdAt"`
	CompletedAt  string               `json:"completedAt,omitempty"`
	DeletedAt    string               `json:"deletedAt,omitempty"`
	Error        string               `json:"error,omitempty"`
	Fingerprint  string               `json:"fingerprint,omitempty"`
	LastModified int64                `json:"lastModified,omitempty"`
}

type LinkPart struct {
	Index  int    `json:"index"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	URL    string `json:"url"`
}
type LinkAsset struct {
	ID          string     `json:"id"`
	Filename    string     `json:"filename"`
	Size        int64      `json:"size"`
	Parts       []LinkPart `json:"parts"`
	CreatedAt   string     `json:"createdAt"`
	ContentType string     `json:"contentType"`
}
type LinkPlaylist struct {
	ID        string `json:"id"`
	Filename  string `json:"filename"`
	Content   string `json:"content"`
	CreatedAt string `json:"createdAt"`
}

type App struct {
	cfg           Config
	client        *http.Client
	uploadSem     chan struct{}
	foregroundSem chan struct{}
	prefetchSem   chan struct{}

	manifestMu    sync.Map // id -> *sync.Mutex
	manifestCache sync.Map // id -> *Manifest

	memMu    sync.Mutex
	mem      map[string]*memItem
	memLRU   *list.List
	memBytes int64

	diskMu    sync.Mutex
	disk      map[string]*diskEntry
	diskBytes int64
	diskHeap  diskHeap

	inflightMu sync.Mutex
	inflight   map[string]*inflight

	prefetchMu  sync.Mutex
	prefetch    map[string]struct{}
	uploadTasks sync.Map // id:index -> *uploadTask
}

type memItem struct {
	key  string
	data []byte
	elem *list.Element
}
type diskEntry struct {
	key, id, path string
	index         int
	size          int64
	lastAccess    time.Time
	item          *diskHeapItem
}
type diskHeapItem struct {
	entry *diskEntry
	index int
}
type diskHeap []*diskHeapItem

func (h diskHeap) Len() int           { return len(h) }
func (h diskHeap) Less(i, j int) bool { return h[i].entry.lastAccess.Before(h[j].entry.lastAccess) }
func (h diskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *diskHeap) Push(x interface{}) {
	it := x.(*diskHeapItem)
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *diskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	it.index = -1
	return it
}

type inflight struct {
	done chan struct{}
	data []byte
	err  error
}
type uploadTask struct {
	done   chan struct{}
	result map[string]interface{}
	err    error
}

func newApp(cfg Config) *App {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   256,
		MaxConnsPerHost:       256,
		DisableCompression:    true,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: cfg.RemoteTimeout,
	}
	return &App{
		cfg: cfg, client: &http.Client{Transport: tr, Timeout: cfg.RemoteTimeout + 10*time.Second},
		uploadSem:     make(chan struct{}, cfg.UploadConcurrency),
		foregroundSem: make(chan struct{}, max(1, cfg.RemoteConcurrency-cfg.PrefetchConcurrency)),
		prefetchSem:   make(chan struct{}, cfg.PrefetchConcurrency),
		mem:           make(map[string]*memItem), memLRU: list.New(),
		disk:     make(map[string]*diskEntry),
		inflight: make(map[string]*inflight),
		prefetch: make(map[string]struct{}),
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (a *App) dirs() (string, string, string) {
	return filepath.Join(a.cfg.StorageDir, "manifests"), filepath.Join(a.cfg.StorageDir, "uploads"), filepath.Join(a.cfg.StorageDir, "cache")
}
func (a *App) init() error {
	m, u, c := a.dirs()
	for _, d := range []string{m, u, c} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return err
		}
	}
	return a.initDiskCache()
}

func uuidv4() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]), hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16]))
}
func nowISO() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func sanitizeFilename(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, s)
	repl := strings.NewReplacer("/", "_", "\\", "_", "?", "_", "%", "_", "*", "_", ";", "_", ":", "_", "|", "_", "\"", "_", "<", "_", ">", "_")
	s = strings.TrimSpace(repl.Replace(s))
	if s == "" {
		s = "file"
	}
	r := []rune(s)
	if len(r) > 255 {
		s = string(r[:255])
	}
	return s
}
func extension(s string) string { return strings.ToLower(filepath.Ext(sanitizeFilename(s))) }

var mimeByExt = map[string]string{
	".mp4": "video/mp4", ".m4v": "video/x-m4v", ".mov": "video/quicktime", ".mkv": "video/x-matroska", ".webm": "video/webm", ".avi": "video/x-msvideo", ".wmv": "video/x-ms-wmv", ".flv": "video/x-flv", ".ts": "video/mp2t", ".mts": "video/mp2t", ".m2ts": "video/mp2t", ".3gp": "video/3gpp", ".3g2": "video/3gpp2", ".ogv": "video/ogg",
	".mp3": "audio/mpeg", ".wav": "audio/wav", ".flac": "audio/flac", ".aac": "audio/aac", ".ogg": "audio/ogg", ".oga": "audio/ogg", ".opus": "audio/opus", ".m4a": "audio/mp4", ".wma": "audio/x-ms-wma",
	".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp", ".svg": "image/svg+xml", ".ico": "image/x-icon", ".tif": "image/tiff", ".tiff": "image/tiff", ".avif": "image/avif", ".pdf": "application/pdf",
}

func mimeType(filename, browser string) string {
	if v := mimeByExt[extension(filename)]; v != "" {
		return v
	}
	if browser != "" && browser != "application/octet-stream" {
		return browser
	}
	return "application/octet-stream"
}
func validID(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func (a *App) manifestPath(id string) string {
	m, _, _ := a.dirs()
	return filepath.Join(m, id+".json")
}
func (a *App) saveManifest(m *Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	p := a.manifestPath(m.ID)
	tmp := fmt.Sprintf("%s.%d.%d.tmp", p, os.Getpid(), time.Now().UnixNano())
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err = os.Rename(tmp, p); err != nil {
		return err
	}
	a.manifestCache.Store(m.ID, m)
	return nil
}
func (a *App) getManifest(id string) (*Manifest, error) {
	if !validID(id) {
		return nil, nil
	}
	if v, ok := a.manifestCache.Load(id); ok {
		return v.(*Manifest), nil
	}
	b, err := os.ReadFile(a.manifestPath(id))
	if err != nil {
		return nil, nil
	}
	var m Manifest
	if json.Unmarshal(b, &m) != nil {
		return nil, nil
	}
	if m.Chunks == nil {
		m.Chunks = map[string]ChunkInfo{}
	}
	a.manifestCache.Store(id, &m)
	return &m, nil
}

func cloneManifest(m *Manifest) *Manifest {
	if m == nil {
		return nil
	}
	c := *m
	c.Chunks = make(map[string]ChunkInfo, len(m.Chunks))
	for k, v := range m.Chunks {
		c.Chunks[k] = v
	}
	return &c
}

func (a *App) withManifestLock(id string, fn func() error) error {
	v, _ := a.manifestMu.LoadOrStore(id, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	return fn()
}

func createManifest(id, filename string, size int64, contentType, fingerprint string, lastModified int64) *Manifest {
	fn := sanitizeFilename(filename)
	return &Manifest{ID: id, Filename: fn, Extension: extension(fn), ContentType: mimeType(fn, contentType), Size: size, ChunkSize: chunkSize, ChunkCount: int((size + chunkSize - 1) / chunkSize), Chunks: map[string]ChunkInfo{}, Status: "uploading", CreatedAt: nowISO(), Fingerprint: strings.TrimSpace(fingerprint), LastModified: lastModified}
}

func (a *App) memoryGet(key string) []byte {
	a.memMu.Lock()
	defer a.memMu.Unlock()
	it := a.mem[key]
	if it == nil {
		return nil
	}
	a.memLRU.MoveToBack(it.elem)
	return it.data
}
func (a *App) memorySet(key string, data []byte) {
	a.memMu.Lock()
	defer a.memMu.Unlock()
	if old := a.mem[key]; old != nil {
		a.memBytes -= int64(len(old.data))
		a.memLRU.Remove(old.elem)
		delete(a.mem, key)
	}
	it := &memItem{key: key, data: data}
	it.elem = a.memLRU.PushBack(it)
	a.mem[key] = it
	a.memBytes += int64(len(data))
	for a.memBytes > memoryCacheMax && a.memLRU.Len() > 0 {
		e := a.memLRU.Front()
		x := e.Value.(*memItem)
		delete(a.mem, x.key)
		a.memLRU.Remove(e)
		a.memBytes -= int64(len(x.data))
	}
}

func cacheKey(id string, index int) string { return fmt.Sprintf("%s:%d", id, index) }
func (a *App) diskPath(id string, index int) string {
	_, _, c := a.dirs()
	return filepath.Join(c, id, fmt.Sprintf("%d.bin", index))
}
func (a *App) initDiskCache() error {
	_, _, c := a.dirs()
	ents, err := os.ReadDir(c)
	if err != nil {
		return err
	}
	count := 0
	a.diskMu.Lock()
	defer a.diskMu.Unlock()
	for _, d := range ents {
		if !d.IsDir() || !validID(d.Name()) {
			continue
		}
		ds, _ := os.ReadDir(filepath.Join(c, d.Name()))
		for _, e := range ds {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".bin") {
				continue
			}
			idx, er := strconv.Atoi(strings.TrimSuffix(e.Name(), ".bin"))
			if er != nil || idx < 0 {
				continue
			}
			p := filepath.Join(c, d.Name(), e.Name())
			st, er := os.Stat(p)
			if er != nil || st.Size() <= 0 {
				continue
			}
			k := cacheKey(d.Name(), idx)
			de := &diskEntry{key: k, id: d.Name(), index: idx, path: p, size: st.Size(), lastAccess: st.ModTime()}
			de.item = &diskHeapItem{entry: de}
			a.disk[k] = de
			a.diskBytes += de.size
			heap.Push(&a.diskHeap, de.item)
			count++
		}
	}
	go a.evictDisk()
	log.Printf("[CACHE] disk index=%d bytes=%d", count, a.diskBytes)
	return nil
}
func (a *App) touchDisk(de *diskEntry) {
	a.diskMu.Lock()
	de.lastAccess = time.Now()
	if de.item != nil {
		heap.Fix(&a.diskHeap, de.item.index)
	}
	a.diskMu.Unlock()
}
func (a *App) removeDisk(k string) {
	a.diskMu.Lock()
	de := a.disk[k]
	if de != nil {
		delete(a.disk, k)
		a.diskBytes -= de.size
		if de.item != nil && de.item.index >= 0 {
			heap.Remove(&a.diskHeap, de.item.index)
		}
	}
	a.diskMu.Unlock()
	if de != nil {
		_ = os.Remove(de.path)
	}
}

func (a *App) readDisk(id string, index int) []byte {
	k := cacheKey(id, index)
	a.diskMu.Lock()
	de := a.disk[k]
	if de == nil {
		a.diskMu.Unlock()
		return nil
	}
	p := de.path
	a.diskMu.Unlock()
	b, err := os.ReadFile(p)
	if err != nil || int64(len(b)) != de.size {
		a.removeDisk(k)
		return nil
	}
	a.touchDisk(de)
	return b
}

// readDiskRange reads only the requested byte range from a cached chunk.
// It avoids loading the entire 5 MiB chunk just to satisfy a small player Range.
func (a *App) readDiskRange(id string, index int, off, length int64) ([]byte, bool) {
	if length <= 0 || off < 0 {
		return nil, false
	}
	k := cacheKey(id, index)
	a.diskMu.Lock()
	de := a.disk[k]
	if de != nil {
		de.lastAccess = time.Now()
		if de.item != nil {
			heap.Fix(&a.diskHeap, de.item.index)
		}
	}
	a.diskMu.Unlock()
	if de == nil || off >= de.size {
		return nil, false
	}
	if off+length > de.size {
		length = de.size - off
	}
	f, err := os.Open(de.path)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	out := make([]byte, length)
	buf := rangeBufPool.Get().([]byte)
	defer rangeBufPool.Put(buf)
	var pos int64
	for pos < length {
		want := int64(len(buf))
		if want > length-pos {
			want = length - pos
		}
		n, er := f.ReadAt(buf[:want], off+pos)
		if n > 0 {
			copy(out[pos:pos+int64(n)], buf[:n])
			pos += int64(n)
		}
		if er != nil {
			if er == io.EOF && pos == length {
				break
			}
			return nil, false
		}
	}
	return out, true
}

func (a *App) writeDisk(id string, index int, data []byte) {
	dir := filepath.Dir(a.diskPath(id, index))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}
	target := a.diskPath(id, index)
	tmp := fmt.Sprintf("%s.%d.%d.tmp", target, os.Getpid(), time.Now().UnixNano())
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return
	}
	k := cacheKey(id, index)
	a.diskMu.Lock()
	if old := a.disk[k]; old != nil {
		a.diskBytes -= old.size
		if old.item != nil && old.item.index >= 0 {
			heap.Remove(&a.diskHeap, old.item.index)
		}
	}
	de := &diskEntry{key: k, id: id, index: index, path: target, size: int64(len(data)), lastAccess: time.Now()}
	de.item = &diskHeapItem{entry: de}
	a.disk[k] = de
	a.diskBytes += de.size
	heap.Push(&a.diskHeap, de.item)
	a.diskMu.Unlock()
	go a.evictDisk()
}
func (a *App) evictDisk() {
	a.diskMu.Lock()
	var remove []*diskEntry
	for a.diskBytes > diskCacheMax && a.diskHeap.Len() > 0 {
		it := heap.Pop(&a.diskHeap).(*diskHeapItem)
		de := it.entry
		if cur := a.disk[de.key]; cur != de {
			continue
		}
		delete(a.disk, de.key)
		a.diskBytes -= de.size
		remove = append(remove, de)
	}
	a.diskMu.Unlock()
	for _, de := range remove {
		_ = os.Remove(de.path)
	}
}
func (a *App) diskStats() (int64, int) {
	a.diskMu.Lock()
	de := a.diskBytes
	n := len(a.disk)
	a.diskMu.Unlock()
	return de, n
}
func (a *App) memStats() (int64, int) {
	a.memMu.Lock()
	b := a.memBytes
	n := len(a.mem)
	a.memMu.Unlock()
	return b, n
}

func (a *App) acquire(ch chan struct{}) { ch <- struct{}{} }
func (a *App) release(ch chan struct{}) { <-ch }

func (a *App) dingHeaders() http.Header {
	h := make(http.Header)
	h.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36")
	h.Set("Referer", "https://www.dingtalk.com")
	h.Set("Origin", "https://www.dingtalk.com")
	if a.cfg.DingTalkCookie != "" {
		h.Set("Cookie", a.cfg.DingTalkCookie)
	}
	if a.cfg.DingTalkToken != "" {
		h.Set("Authorization", "Bearer "+a.cfg.DingTalkToken)
	}
	return h
}
func findURL(v interface{}, seen map[uintptr]bool) string {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(strings.ToLower(x), "http://") || strings.HasPrefix(strings.ToLower(x), "https://") {
			return x
		}
	case map[string]interface{}:
		for _, k := range []string{"url", "src", "imageUrl", "imgUrl", "picUrl", "downloadUrl", "fileUrl", "fileUrlPath"} {
			if s, ok := x[k].(string); ok && (strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")) {
				return s
			}
		}
		for _, k := range []string{"data", "result"} {
			if y, ok := x[k]; ok {
				if s := findURL(y, seen); s != "" {
					return s
				}
			}
		}
		for _, y := range x {
			if s := findURL(y, seen); s != "" {
				return s
			}
		}
	}
	return ""
}
func retryable(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, x := range []string{"timeout", "timed out", "connection reset", "connection refused", "no such host", "network", "eof", "tls"} {
		if strings.Contains(s, x) {
			return true
		}
	}
	return false
}
func sleepRetry(base time.Duration, attempt int) {
	d := base * time.Duration(1<<min(attempt-1, 4))
	if d > 15*time.Second {
		d = 15 * time.Second
	}
	time.Sleep(d + time.Duration(time.Now().UnixNano()%750)*time.Millisecond)
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (a *App) uploadBytesToDingTalk(data []byte, index int) (string, error) {
	var last error
	for attempt := 1; attempt <= a.cfg.RemoteRetryCount; attempt++ {
		a.acquire(a.uploadSem)
		body := &bytes.Buffer{}
		mw := multipart.NewWriter(body)
		part, err := mw.CreateFormFile("picFile", fmt.Sprintf("chunk_%08d.jpg", index))
		if err == nil {
			_, err = part.Write(data)
		}
		if err == nil {
			err = mw.Close()
		} else {
			_ = mw.Close()
		}
		if err != nil {
			a.release(a.uploadSem)
			last = err
			break
		}
		req, err := http.NewRequest("POST", a.cfg.DingTalkUploadURL, bytes.NewReader(body.Bytes()))
		if err == nil {
			for k, v := range a.dingHeaders() {
				req.Header[k] = v
			}
			req.Header.Set("Content-Type", mw.FormDataContentType())
			req.ContentLength = int64(body.Len())
			resp, er := a.client.Do(req)
			if er == nil {
				rb, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
				resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					var v interface{}
					if json.Unmarshal(rb, &v) == nil {
						u := findURL(v, map[uintptr]bool{})
						if u != "" {
							a.release(a.uploadSem)
							return u, nil
						}
					}
					er = fmt.Errorf("DingTalk 返回结果中没有 URL: %s", truncate(string(rb), 1000))
				} else {
					er = fmt.Errorf("DingTalk HTTP %d: %s", resp.StatusCode, truncate(string(rb), 1000))
				}
				err = er
			} else {
				err = er
			}
		}
		a.release(a.uploadSem)
		last = err
		if attempt < a.cfg.RemoteRetryCount && retryable(err) {
			log.Printf("[DINGTALK RETRY] chunk=%d attempt=%d/%d err=%v", index, attempt, a.cfg.RemoteRetryCount, err)
			sleepRetry(a.cfg.RemoteRetryBaseDelay, attempt)
			continue
		}
		break
	}
	return "", last
}

var rangeBufPool = sync.Pool{
	New: func() interface{} { return make([]byte, 256*1024) },
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func chunkRemoteCandidates(c ChunkInfo) []string {
	out := make([]string, 0, 3)
	seen := map[string]bool{}
	for _, u := range []string{c.URL, c.DingTalkURL} {
		if u != "" && !seen[u] {
			out = append(out, u)
			seen[u] = true
		}
	}
	return out
}
func isurl360(u string) bool {
	return strings.Contains(strings.ToLower(u), "qhimg.com") || strings.Contains(strings.ToLower(u), "e.360.cn")
}
func remoteRangeFor(u string, start, end int64) (int64, int64) {
	if isurl360(u) {
		return start + 125, end + 125
	}
	return start, end
}

func remoteHeadersFor(u string, a *App) http.Header {
	if strings.Contains(u, "assets-cdn.salesmartly.com") {
		h := make(http.Header)
		h.Set("User-Agent", "Mozilla/5.0")
		return h
	}
	return a.dingHeaders()
}
func (a *App) fetchChunk(ctx context.Context, m *Manifest, index int, foreground bool) ([]byte, string, error) {
	c, ok := m.Chunks[strconv.Itoa(index)]
	if !ok {
		return nil, "", fmt.Errorf("Chunk %d 不存在", index)
	}
	expected := min64(m.ChunkSize, m.Size-int64(index)*m.ChunkSize)
	sem := a.prefetchSem
	if foreground {
		sem = a.foregroundSem
	}
	a.acquire(sem)
	defer a.release(sem)
	candidates := chunkRemoteCandidates(c)
	if len(candidates) == 0 {
		return nil, "", errors.New("chunk 没有远端 URL")
	}
	var last error
	for _, u := range candidates {
		headers := remoteHeadersFor(u, a)
		rs, re := remoteRangeFor(u, 0, expected-1)
		headers.Set("Range", fmt.Sprintf("bytes=%d-%d", rs, re))
		for attempt := 1; attempt <= a.cfg.RemoteRetryCount; attempt++ {
			ctx2, cancel := context.WithTimeout(ctx, a.cfg.RemoteTimeout)
			req, err := http.NewRequestWithContext(ctx2, "GET", u, nil)
			if err == nil {
				req.Header = headers
				resp, er := a.client.Do(req)
				if er == nil {
					if resp.StatusCode != http.StatusPartialContent && !(strings.Contains(u, "salesmartly") && resp.StatusCode == http.StatusOK) {
						b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
						resp.Body.Close()
						err = fmt.Errorf("remote HTTP %d %s", resp.StatusCode, truncate(string(b), 500))
					} else {
						data, er2 := io.ReadAll(io.LimitReader(resp.Body, expected+1))
						resp.Body.Close()
						if er2 == nil && int64(len(data)) == expected {
							cancel()
							return data, u, nil
						}
						if er2 == nil {
							er2 = fmt.Errorf("chunk length mismatch: want %d got %d", expected, len(data))
						}
						err = er2
					}
				} else {
					err = er
				}
			}
			cancel()
			last = err
			if attempt < a.cfg.RemoteRetryCount && retryable(err) {
				sleepRetry(a.cfg.RemoteRetryBaseDelay, attempt)
				continue
			}
			break
		}
		log.Printf("[REMOTE FALLBACK] file=%s chunk=%d source=%s failed: %v", m.Filename, index, u, last)
	}
	return nil, "", last
}

func (a *App) fetchChunkRange(ctx context.Context, m *Manifest, index int, relStart, relEnd int64) ([]byte, error) {
	c, ok := m.Chunks[strconv.Itoa(index)]
	if !ok {
		return nil, fmt.Errorf("Chunk %d 不存在", index)
	}
	if relStart < 0 || relEnd < relStart {
		return nil, errors.New("invalid chunk range")
	}
	expected := min64(m.ChunkSize, m.Size-int64(index)*m.ChunkSize)
	if relEnd >= expected {
		relEnd = expected - 1
	}
	want := relEnd - relStart + 1
	a.acquire(a.foregroundSem)
	defer a.release(a.foregroundSem)
	var last error
	for _, u := range chunkRemoteCandidates(c) {
		headers := remoteHeadersFor(u, a)
		rs, re := remoteRangeFor(u, relStart, relEnd)
		headers.Set("Range", fmt.Sprintf("bytes=%d-%d", rs, re))
		for attempt := 1; attempt <= a.cfg.RemoteRetryCount; attempt++ {
			ctx2, cancel := context.WithTimeout(ctx, a.cfg.RemoteTimeout)
			req, err := http.NewRequestWithContext(ctx2, "GET", u, nil)
			if err == nil {
				req.Header = headers
				resp, er := a.client.Do(req)
				if er == nil {
					if resp.StatusCode != http.StatusPartialContent && !(strings.Contains(u, "salesmartly") && resp.StatusCode == http.StatusOK) {
						b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
						resp.Body.Close()
						err = fmt.Errorf("remote partial HTTP %d: %s", resp.StatusCode, truncate(string(b), 500))
					} else {
						data, er2 := io.ReadAll(io.LimitReader(resp.Body, want+1))
						resp.Body.Close()
						if er2 == nil && int64(len(data)) == want {
							cancel()
							return data, nil
						}
						if er2 == nil {
							er2 = fmt.Errorf("partial length mismatch: want %d got %d", want, len(data))
						}
						err = er2
					}
				} else {
					err = er
				}
			}
			cancel()
			last = err
			if attempt < a.cfg.RemoteRetryCount && retryable(err) {
				sleepRetry(a.cfg.RemoteRetryBaseDelay, attempt)
				continue
			}
			break
		}
		log.Printf("[RANGE FALLBACK] file=%s chunk=%d source=%s failed: %v", m.Filename, index, u, last)
	}
	return nil, last
}

// streamChunkRange streams an uncached seek range directly from DingTalk to the
// player. It deliberately does not io.ReadAll the requested range first. This
// keeps the first-byte path independent from the 5 MiB cache-warming path.
func (a *App) streamChunkRange(ctx context.Context, w http.ResponseWriter, m *Manifest, index int, relStart, relEnd int64) error {
	c, ok := m.Chunks[strconv.Itoa(index)]
	if !ok {
		return fmt.Errorf("Chunk %d 不存在", index)
	}
	if relStart < 0 || relEnd < relStart {
		return errors.New("invalid chunk range")
	}
	expected := min64(m.ChunkSize, m.Size-int64(index)*m.ChunkSize)
	if relEnd >= expected {
		relEnd = expected - 1
	}
	want := relEnd - relStart + 1
	a.acquire(a.foregroundSem)
	defer a.release(a.foregroundSem)
	var last error
	for _, u := range chunkRemoteCandidates(c) {
		headers := remoteHeadersFor(u, a)
		rs, re := remoteRangeFor(u, relStart, relEnd)
		headers.Set("Range", fmt.Sprintf("bytes=%d-%d", rs, re))
		for attempt := 1; attempt <= a.cfg.RemoteRetryCount; attempt++ {
			started := time.Now()
			ctx2, cancel := context.WithTimeout(ctx, a.cfg.RemoteTimeout)
			req, err := http.NewRequestWithContext(ctx2, "GET", u, nil)
			if err == nil {
				req.Header = headers
				resp, er := a.client.Do(req)
				remoteTTFB := time.Since(started)
				if er == nil {
					if resp.StatusCode != http.StatusPartialContent && !(strings.Contains(u, "salesmartly") && resp.StatusCode == http.StatusOK) {
						b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
						resp.Body.Close()
						err = fmt.Errorf("remote partial HTTP %d: %s", resp.StatusCode, truncate(string(b), 500))
					} else {
						lr := io.LimitReader(resp.Body, want)
						n, copyErr := io.CopyBuffer(w, lr, make([]byte, 64*1024))
						resp.Body.Close()
						if f, ok := w.(http.Flusher); ok {
							f.Flush()
						}
						if copyErr == nil && n == want {
							log.Printf("[SEEK STREAM] file=%s chunk=%d source=%s range=%d-%d remote_ttfb=%s total=%s bytes=%d", m.Filename, index, u, relStart, relEnd, remoteTTFB.Round(time.Millisecond), time.Since(started).Round(time.Millisecond), n)
							cancel()
							return nil
						}
						if copyErr == nil {
							copyErr = fmt.Errorf("partial stream length mismatch: want %d got %d", want, n)
						}
						err = copyErr
					}
				} else {
					err = er
				}
			}
			cancel()
			last = err
			if attempt < a.cfg.RemoteRetryCount && retryable(err) {
				log.Printf("[SEEK RETRY] file=%s chunk=%d source=%s attempt=%d/%d err=%v", m.Filename, index, u, attempt, a.cfg.RemoteRetryCount, err)
				sleepRetry(a.cfg.RemoteRetryBaseDelay, attempt)
				continue
			}
			break
		}
		log.Printf("[SEEK FALLBACK] file=%s chunk=%d source=%s failed: %v", m.Filename, index, u, last)
	}
	return last
}

func (a *App) getCachedChunk(ctx context.Context, m *Manifest, index int, foreground bool) ([]byte, string, error) {
	k := cacheKey(m.ID, index)

	if b := a.memoryGet(k); b != nil {
		return b, "memory", nil
	}
	if b := a.readDisk(m.ID, index); b != nil {
		a.memorySet(k, b)
		return b, "disk", nil
	}

	a.inflightMu.Lock()
	if in := a.inflight[k]; in != nil {
		a.inflightMu.Unlock()
		<-in.done
		if in.err != nil {
			return nil, "shared-inflight", in.err
		}
		a.memorySet(k, in.data)
		return in.data, "shared-inflight", nil
	}
	in := &inflight{done: make(chan struct{})}
	a.inflight[k] = in
	a.inflightMu.Unlock()

	defer func() {
		a.inflightMu.Lock()
		delete(a.inflight, k)
		close(in.done)
		a.inflightMu.Unlock()
	}()

	if b := a.memoryGet(k); b != nil {
		in.data = b
		return b, "memory", nil
	}
	if b := a.readDisk(m.ID, index); b != nil {
		a.memorySet(k, b)
		in.data = b
		return b, "disk", nil
	}

	b, _, err := a.fetchChunk(ctx, m, index, foreground)
	if err != nil {
		in.err = err
		return nil, "remote", err
	}

	a.memorySet(k, b)
	in.data = b
	// Memory is available immediately; disk cache is intentionally asynchronous.
	go a.writeDisk(m.ID, index, b)
	return b, "remote", nil
}

func chunkBounds(m *Manifest, index int) (int64, int64) {
	s := int64(index) * m.ChunkSize
	e := min64(m.Size-1, s+m.ChunkSize-1)
	return s, e
}
func parseRange(h string, total int64) (*RangeSpec, error) {
	if h == "" {
		return nil, nil
	}
	if !strings.HasPrefix(strings.ToLower(h), "bytes=") {
		return nil, errors.New("Invalid Range")
	}
	v := strings.TrimSpace(strings.TrimPrefix(strings.ToLower(h), "bytes="))
	if strings.Contains(v, ",") {
		return nil, errors.New("Invalid Range")
	}
	p := strings.SplitN(v, "-", 2)
	if len(p) != 2 {
		return nil, errors.New("Invalid Range")
	}
	var s, e int64
	var err error
	if p[0] == "" {
		n, er := strconv.ParseInt(p[1], 10, 64)
		if er != nil || n <= 0 {
			return nil, errors.New("Invalid Range")
		}
		s = max64(0, total-n)
		e = total - 1
	} else {
		s, err = strconv.ParseInt(p[0], 10, 64)
		if err != nil || s < 0 {
			return nil, errors.New("Invalid Range")
		}
		if p[1] == "" {
			e = total - 1
		} else {
			e, err = strconv.ParseInt(p[1], 10, 64)
			if err != nil {
				return nil, errors.New("Invalid Range")
			}
		}
	}
	if s >= total || e < s {
		return nil, errors.New("Range Not Satisfiable")
	}
	if e >= total {
		e = total - 1
	}
	return &RangeSpec{Start: s, End: e, Length: e - s + 1}, nil
}

type RangeSpec struct{ Start, End, Length int64 }

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (a *App) schedulePrefetch(ctx context.Context, m *Manifest, current int) {
	start := current + 1
	if start >= m.ChunkCount {
		return
	}
	key := fmt.Sprintf("%s:%d", m.ID, start)
	a.prefetchMu.Lock()
	if _, ok := a.prefetch[key]; ok {
		a.prefetchMu.Unlock()
		return
	}
	a.prefetch[key] = struct{}{}
	a.prefetchMu.Unlock()
	go func() {
		defer func() { a.prefetchMu.Lock(); delete(a.prefetch, key); a.prefetchMu.Unlock() }()
		end := min(m.ChunkCount, start+int(prefetchBytes/m.ChunkSize))
		jobs := make(chan int)
		var wg sync.WaitGroup
		for i := 0; i < a.cfg.PrefetchConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobs {
					if _, _, err := a.getCachedChunk(ctx, m, idx, false); err != nil {
						log.Printf("[PREFETCH ERROR] %s chunk=%d: %v", m.Filename, idx, err)
					}
				}
			}()
		}
		for i := start; i < end; i++ {
			select {
			case jobs <- i:
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				return
			}
		}
		close(jobs)
		wg.Wait()
	}()
}

func (a *App) streamRange(w http.ResponseWriter, r *http.Request, m *Manifest, start, end int64) error {
	ctx := r.Context()
	first := int(start / m.ChunkSize)
	last := int(end / m.ChunkSize)
	// Seek/initial-play fast path: if the browser asks for a small range inside one uncached chunk,
	// return only the requested bytes immediately and warm the complete chunk in the background.
	if first == last && end-start+1 <= 2*1024*1024 {
		k := cacheKey(m.ID, first)
		if b := a.memoryGet(k); b != nil {
			bs, _ := chunkBounds(m, first)
			part := b[start-bs : end-bs+1]
			_, err := w.Write(part)
			return err
		}
		// Critical fast path: if the whole chunk is already on SSD, read only
		// the requested bytes. Do NOT os.ReadFile the entire 5 MiB chunk.
		bs, _ := chunkBounds(m, first)
		relS := start - bs
		relE := end - bs
		if part, ok := a.readDiskRange(m.ID, first, relS, relE-relS+1); ok {
			_, err := w.Write(part)
			return err
		}
		if err := a.streamChunkRange(ctx, w, m, first, relS, relE); err == nil {
			// Warm full chunk without blocking the player.
			go func() {
				warmCtx, cancel := context.WithTimeout(context.Background(), a.cfg.RemoteTimeout*time.Duration(a.cfg.RemoteRetryCount))
				defer cancel()
				if _, _, e := a.getCachedChunk(warmCtx, m, first, false); e != nil {
					log.Printf("[SEEK WARM ERROR] %s chunk=%d: %v", m.Filename, first, e)
				}
			}()
			a.schedulePrefetch(ctx, m, first)
			return nil
		}
	}
	for idx := first; idx <= last; idx++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b, _, err := a.getCachedChunk(ctx, m, idx, true)
		if err != nil {
			return err
		}
		bs, be := chunkBounds(m, idx)
		as := max64(start, bs)
		ae := min64(end, be)
		part := b[as-bs : ae-bs+1]
		if _, err = w.Write(part); err != nil {
			return err
		}
		if idx == first {
			a.schedulePrefetch(ctx, m, idx)
		}
	}
	return nil
}

func (a *App) fileHeaders(w http.ResponseWriter, m *Manifest) {
	w.Header().Set("Content-Type", m.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, strings.ReplaceAll(m.Filename, `"`, `_`)))
}

func (a *App) handleFile(w http.ResponseWriter, r *http.Request, m *Manifest) {
	if m == nil {
		http.NotFound(w, r)
		return
	}
	if m.Status != "ready" {
		http.Error(w, `{"success":false,"error":"File is not ready"}`, http.StatusConflict)
		return
	}
	a.fileHeaders(w, m)
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
		return
	}
	rs, err := parseRange(r.Header.Get("Range"), m.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", m.Size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if rs == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
		w.WriteHeader(http.StatusOK)
		err = a.streamRange(w, r, m, 0, m.Size-1)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(rs.Length, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rs.Start, rs.End, m.Size))
		w.WriteHeader(http.StatusPartialContent)
		err = a.streamRange(w, r, m, rs.Start, rs.End)
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Printf("[FILE ERROR] %s: %v", m.Filename, err)
	}
}

func jsonResp(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decodeJSON(r *http.Request, v interface{}) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	return dec.Decode(v)
}

func (a *App) findByFingerprint(fp string, size int64) *Manifest {
	fp = strings.TrimSpace(fp)
	if fp == "" || size <= 0 {
		return nil
	}
	ms, _ := a.listManifests()
	var best *Manifest
	for _, m := range ms {
		if m.Status == "deleted" || m.Size != size || m.Fingerprint == "" || m.Fingerprint != fp {
			continue
		}
		if best == nil || m.CreatedAt > best.CreatedAt {
			cp := *m
			best = &cp
		}
	}
	return best
}

func (a *App) createVideo(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Filename     string `json:"filename"`
		Size         int64  `json:"size"`
		ContentType  string `json:"contentType"`
		Fingerprint  string `json:"fingerprint"`
		LastModified int64  `json:"lastModified"`
	}
	if decodeJSON(r, &q) != nil || strings.TrimSpace(q.Filename) == "" || q.Size <= 0 {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "filename 参数错误或 size 参数错误"})
		return
	}
	if existing := a.findByFingerprint(q.Fingerprint, q.Size); existing != nil {
		idx := make([]int, 0, len(existing.Chunks))
		for k := range existing.Chunks {
			i, _ := strconv.Atoi(k)
			idx = append(idx, i)
		}
		sort.Ints(idx)
		jsonResp(w, 200, map[string]interface{}{"success": true, "id": existing.ID, "filename": existing.Filename, "extension": existing.Extension, "contentType": existing.ContentType, "size": existing.Size, "chunkSize": existing.ChunkSize, "chunkCount": existing.ChunkCount, "uploadConcurrency": a.cfg.UploadConcurrency, "resumed": existing.Status != "ready", "alreadyExists": existing.Status == "ready", "uploadedChunks": idx, "status": existing.Status, "url": fileURL(existing)})
		return
	}
	m := createManifest(uuidv4(), q.Filename, q.Size, q.ContentType, q.Fingerprint, q.LastModified)
	if err := a.saveManifest(m); err != nil {
		jsonResp(w, 500, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	jsonResp(w, 200, map[string]interface{}{"success": true, "id": m.ID, "filename": m.Filename, "extension": m.Extension, "contentType": m.ContentType, "size": m.Size, "chunkSize": m.ChunkSize, "chunkCount": m.ChunkCount, "uploadConcurrency": a.cfg.UploadConcurrency, "resumed": false, "alreadyExists": false, "uploadedChunks": []int{}})
}

func (a *App) getVideo(w http.ResponseWriter, r *http.Request, id string) {
	m, _ := a.getManifest(id)
	if m == nil {
		jsonResp(w, 404, map[string]interface{}{"success": false, "error": "文件不存在"})
		return
	}
	idx := make([]int, 0, len(m.Chunks))
	for k := range m.Chunks {
		i, _ := strconv.Atoi(k)
		idx = append(idx, i)
	}
	sort.Ints(idx)
	jsonResp(w, 200, map[string]interface{}{"success": true, "id": m.ID, "filename": m.Filename, "extension": m.Extension, "contentType": m.ContentType, "size": m.Size, "chunkSize": m.ChunkSize, "chunkCount": m.ChunkCount, "status": m.Status, "uploadedChunks": idx, "fingerprint": m.Fingerprint, "resumed": m.Status != "ready"})
}

func (a *App) uploadChunk(w http.ResponseWriter, r *http.Request, id string, index int) {
	if !validID(id) || index < 0 {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "video id 或 chunk index 错误"})
		return
	}
	m, _ := a.getManifest(id)
	if m == nil {
		jsonResp(w, 404, map[string]interface{}{"success": false, "error": "文件不存在"})
		return
	}
	if index >= m.ChunkCount {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "chunk index 超出范围"})
		return
	}

	// 已经上传过的 chunk 直接返回，不重新传钉钉。
	if c, ok := m.Chunks[strconv.Itoa(index)]; ok {
		jsonResp(w, 200, map[string]interface{}{"success": true, "index": index, "size": c.Size, "url": c.URL, "alreadyUploaded": true})
		return
	}

	// 同一 chunk 的重复请求共享一个钉钉上传任务；不同 chunk 可以完全并行。
	taskKey := fmt.Sprintf("%s:%d", id, index)
	v, loaded := a.uploadTasks.LoadOrStore(taskKey, &uploadTask{done: make(chan struct{})})
	task := v.(*uploadTask)
	if loaded {
		<-task.done
		if task.err != nil {
			jsonResp(w, 502, map[string]interface{}{"success": false, "error": task.err.Error()})
			return
		}
		jsonResp(w, 200, task.result)
		return
	}
	defer a.uploadTasks.Delete(taskKey)
	defer close(task.done)

	// 其他请求可能刚刚完成了这个 chunk。
	if latest, _ := a.getManifest(id); latest != nil {
		if c, ok := latest.Chunks[strconv.Itoa(index)]; ok {
			task.result = map[string]interface{}{"success": true, "index": index, "size": c.Size, "url": c.URL, "alreadyUploaded": true}
			jsonResp(w, 200, task.result)
			return
		}
		m = latest
	}

	expected := min64(m.ChunkSize, m.Size-int64(index)*m.ChunkSize)
	if expected <= 0 {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "chunk 大小无效"})
		return
	}

	// 浏览器直接上传原始 5 MiB chunk，由钉钉作为当前分片存储。
	// 使用 MultipartReader，避免 ParseMultipartForm 把大分片额外落到临时磁盘。
	mr, err := r.MultipartReader()
	if err != nil {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "multipart 解析失败: " + err.Error()})
		return
	}
	var data []byte
	found := false
	for {
		part, e := mr.NextPart()
		if e == io.EOF {
			break
		}
		if e != nil {
			jsonResp(w, 400, map[string]interface{}{"success": false, "error": e.Error()})
			return
		}
		if part.FormName() != "picFile" {
			_, _ = io.Copy(io.Discard, part)
			continue
		}
		found = true
		data, err = io.ReadAll(io.LimitReader(part, expected+1))
		part.Close()
		if err != nil {
			jsonResp(w, 500, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		break
	}
	if !found {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "没有收到 picFile"})
		return
	}
	if int64(len(data)) != expected {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": fmt.Sprintf("chunk 大小错误：期望 %d，实际 %d", expected, len(data))})
		return
	}

	// 浏览器仍严格按 5 MiB 分片；钉钉立即保存每个分片。
	// 保留本地分片用于断点续传和读取回退。
	partDir := filepath.Join(a.cfg.StorageDir, "uploads", id)
	if err := os.MkdirAll(partDir, 0700); err != nil {
		task.err = err
		jsonResp(w, 500, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	partPath := filepath.Join(partDir, fmt.Sprintf("%08d.part", index))
	tmpPart := partPath + ".tmp"
	if err := os.WriteFile(tmpPart, data, 0600); err != nil {
		task.err = err
		jsonResp(w, 500, map[string]interface{}{"success": false, "error": "保存分片失败: " + err.Error()})
		return
	}
	if err := os.Rename(tmpPart, partPath); err != nil {
		_ = os.Remove(tmpPart)
		task.err = err
		jsonResp(w, 500, map[string]interface{}{"success": false, "error": "提交分片失败: " + err.Error()})
		return
	}
	// 360 is the primary store. The uploaded object contains a 125-byte prefix;
	// readers compensate for it when issuing Range requests.
	ctx360, cancel360 := context.WithTimeout(r.Context(), a.cfg.RemoteTimeout)
	url360, err360 := uploadOne360(ctx360, a.client, data, uuidv4()+".png")
	cancel360()
	var dingURL string
	var dingErr error
	primary := ""
	source := "360_CHUNK"
	if err360 == nil {
		primary = url360
		log.Printf("[360 UPLOAD OK] file=%s chunk=%d bytes=%d url=%s", m.Filename, index, len(data), url360)
	} else {
		log.Printf("[360 UPLOAD FAILED] file=%s chunk=%d bytes=%d err=%v; trying DingTalk fallback", m.Filename, index, len(data), err360)
		dingURL, dingErr = a.uploadBytesToDingTalk(data, index)
		if dingErr != nil {
			task.err = fmt.Errorf("360 上传失败: %v; 钉钉备用上传也失败: %v", err360, dingErr)
			jsonResp(w, 502, map[string]interface{}{"success": false, "error": task.err.Error(), "360_error": err360.Error(), "dingtalk_error": dingErr.Error()})
			return
		}
		primary = dingURL
		source = "DINGTALK_FALLBACK"
	}

	var result map[string]interface{}
	err = a.withManifestLock(id, func() error {
		latest, _ := a.getManifest(id)
		if latest == nil {
			return errors.New("文件不存在")
		}
		if c, ok := latest.Chunks[strconv.Itoa(index)]; ok {
			result = map[string]interface{}{"success": true, "index": index, "size": c.Size, "url": c.URL, "alreadyUploaded": true}
			return nil
		}
		updated := cloneManifest(latest)
		updated.Chunks[strconv.Itoa(index)] = ChunkInfo{Index: index, Size: expected, URL: primary, UploadedAt: nowISO(), Source: source, DingTalkURL: dingURL}
		result = map[string]interface{}{"success": true, "index": index, "size": expected, "url": primary, "source": source, "dingtalk_url": dingURL, "360_error": errString(err360), "dingtalk_error": errString(dingErr), "alreadyUploaded": false}
		return a.saveManifest(updated)
	})
	if err != nil {
		task.err = err
		jsonResp(w, 500, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	task.result = result
	jsonResp(w, 200, result)
}

func (a *App) complete(w http.ResponseWriter, r *http.Request, id string) {
	m, _ := a.getManifest(id)
	if m == nil {
		jsonResp(w, 404, map[string]interface{}{"success": false, "error": "文件不存在"})
		return
	}
	var missing []int
	var invalid []map[string]interface{}
	for i := 0; i < m.ChunkCount; i++ {
		c, ok := m.Chunks[strconv.Itoa(i)]
		if !ok {
			missing = append(missing, i)
			continue
		}
		expected := min64(m.ChunkSize, m.Size-int64(i)*m.ChunkSize)
		if c.Size != expected {
			invalid = append(invalid, map[string]interface{}{"index": i, "expected": expected, "actual": c.Size})
		}
	}
	if len(missing) > 0 || len(invalid) > 0 {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "还有 chunk 未上传或 chunk 大小异常", "missing": missing, "invalid": invalid})
		return
	}
	var updated *Manifest
	if err := a.withManifestLock(id, func() error {
		latest, _ := a.getManifest(id)
		if latest == nil {
			return errors.New("文件不存在")
		}
		updated = cloneManifest(latest)
		updated.Status = "ready"
		updated.CompletedAt = nowISO()
		updated.Error = ""
		return a.saveManifest(updated)
	}); err != nil {
		jsonResp(w, 500, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	jsonResp(w, 200, map[string]interface{}{"success": true, "id": updated.ID, "filename": updated.Filename, "extension": updated.Extension, "contentType": updated.ContentType, "size": updated.Size, "chunkSize": updated.ChunkSize, "chunkCount": updated.ChunkCount, "status": updated.Status, "url": fileURL(updated)})
}

func (a *App) progress(w http.ResponseWriter, r *http.Request, id string) {
	m, _ := a.getManifest(id)
	if m == nil {
		jsonResp(w, 404, map[string]interface{}{"success": false, "error": "Not Found"})
		return
	}
	done := len(m.Chunks)
	total := m.ChunkCount
	jsonResp(w, 200, map[string]interface{}{
		"success":         true,
		"id":              m.ID,
		"filename":        m.Filename,
		"extension":       m.Extension,
		"contentType":     m.ContentType,
		"status":          m.Status,
		"total":           total,
		"completed":       done,
		"pending":         max(0, total-done),
		"percent":         float64(done) / float64(max(1, total)) * 100,
		"size":            m.Size,
		"completedAt":     m.CompletedAt,
		"error":           m.Error,
		"browserAccepted": done,
		"done":            done,
		"processing":      0,
		"queued":          0,
		"failed":          0,
		"url":             fileURL(m),
		"videoUrl":        fileURL(m),
	})
}

func (a *App) apiFiles(w http.ResponseWriter, r *http.Request) {
	m, _ := a.listManifests()
	type F struct {
		ID             string  `json:"id"`
		Filename       string  `json:"filename"`
		ContentType    string  `json:"contentType"`
		Status         string  `json:"status"`
		CreatedAt      string  `json:"createdAt"`
		Size           int64   `json:"size"`
		ChunkCount     int     `json:"chunkCount"`
		ChunksUploaded int     `json:"chunksUploaded"`
		CompletedAt    *string `json:"completedAt"`
		URL            string  `json:"url"`
	}
	out := make([]F, 0)
	for _, x := range m {
		if x.Status == "deleted" {
			continue
		}
		ca := x.CompletedAt
		out = append(out, F{ID: x.ID, Filename: x.Filename, ContentType: x.ContentType, Status: x.Status, CreatedAt: x.CreatedAt, Size: x.Size, ChunkCount: x.ChunkCount, ChunksUploaded: len(x.Chunks), CompletedAt: (func() *string {
			if ca == "" {
				return nil
			}
			return &ca
		})(), URL: fileURL(x)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	jsonResp(w, 200, map[string]interface{}{"success": true, "count": len(out), "files": out})
}
func (a *App) listManifests() ([]*Manifest, error) {
	dir, _, _ := a.dirs()
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]*Manifest, 0, len(ents))
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		m, _ := a.getManifest(id)
		if m != nil {
			out = append(out, m)
		}
	}
	return out, nil
}

func escapeHTML(s string) string     { return templateEscape(s) }
func templateEscape(s string) string { return htmlReplacer.Replace(s) }

var htmlReplacer = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#39;")

func fmtSize(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	if n < 1<<20 {
		return fmt.Sprintf("%.1f KiB", float64(n)/1024)
	}
	if n < 1<<30 {
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
}

// fileURL returns the single persistent public URL for a file, preserving the original extension.
// Example: /file/<uuid>.mp4, /file/<uuid>.mp3, /file/<uuid>.jpg
func fileURL(m *Manifest) string {
	if m == nil {
		return ""
	}
	return "/file/" + url.PathEscape(m.ID) + m.Extension
}

func (a *App) filesPage(w http.ResponseWriter, r *http.Request) {
	ms, _ := a.listManifests()
	sort.Slice(ms, func(i, j int) bool { return ms[i].CreatedAt > ms[j].CreatedAt })
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>已上传文件</title><style>body{font-family:system-ui;margin:20px;background:#f5f6f8}table{width:100%;border-collapse:collapse;background:#fff}th,td{padding:12px;border-bottom:1px solid #ddd;text-align:left}a{color:#1677ff}</style></head><body><h2>当前已上传文件 (`+strconv.Itoa(len(ms))+`)</h2><table><thead><tr><th>文件</th><th>大小</th><th>状态</th><th>分片</th><th>操作</th></tr></thead><tbody>`)
	for _, m := range ms {
		if m.Status == "deleted" {
			continue
		}
		fmt.Fprintf(w, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%d/%d</td><td><a href=\"%s\" target=\"_blank\">打开</a></td></tr>", escapeHTML(m.Filename), fmtSize(m.Size), escapeHTML(m.Status), len(m.Chunks), m.ChunkCount, escapeHTML(fileURL(m)))
	}
	io.WriteString(w, `</tbody></table></body></html>`)
}

func (a *App) status(w http.ResponseWriter, r *http.Request) {
	mb, me := a.memStats()
	db, de := a.diskStats()
	m := map[string]interface{}{"success": true, "service": "video-stream-server-go", "architecture": map[string]interface{}{"upload": "CHUNKED", "uploadChunkSize": chunkSize, "uploadConcurrency": a.cfg.UploadConcurrency, "storage": "MANIFEST + SHARED CHUNK CACHE", "firstOpen": "CACHE_FIRST + PREFETCH", "range": "CACHE_FIRST + RANGE_COALESCE", "seek": "CACHE_FIRST", "sharedCache": true, "seekCache": true, "diskCache": true, "fullFileMemoryCache": false, "globalRemoteQueue": true, "globalRemoteConcurrencyLimit": a.cfg.RemoteConcurrency, "remoteRange": "DINGTALK_RANGE_206_STREAM", "runtime": "Go", "gomaxprocs": runtime.GOMAXPROCS(0)}, "cache": map[string]interface{}{"memoryLimitBytes": memoryCacheMax, "memoryBytes": mb, "memoryEntries": me, "diskLimitBytes": diskCacheMax, "diskBytes": db, "diskEntries": de, "prefetchBytes": prefetchBytes, "prefetchChunks": int((prefetchBytes + chunkSize - 1) / chunkSize), "inflightChunks": a.inflightCount(), "prefetchJobs": a.prefetchCount()}, "chunkSize": chunkSize, "remoteTimeout": int64(a.cfg.RemoteTimeout / time.Millisecond), "remoteProxyMode": "RANGE_STREAM_PLUS_BACKGROUND_CHUNK_CACHE", "memory": map[string]interface{}{"alloc": mb, "goroutines": runtime.NumGoroutine(), "numGC": func() uint32 { var ms runtime.MemStats; runtime.ReadMemStats(&ms); return ms.NumGC }()}}
	jsonResp(w, 200, m)
}
func (a *App) inflightCount() int {
	a.inflightMu.Lock()
	n := len(a.inflight)
	a.inflightMu.Unlock()
	return n
}
func (a *App) prefetchCount() int {
	a.prefetchMu.Lock()
	n := len(a.prefetch)
	a.prefetchMu.Unlock()
	return n
}
func (a *App) cacheStatus(w http.ResponseWriter, r *http.Request) {
	mb, me := a.memStats()
	db, de := a.diskStats()
	jsonResp(w, 200, map[string]interface{}{"success": true, "limits": map[string]interface{}{"memoryBytes": memoryCacheMax, "memoryMiB": float64(memoryCacheMax) / (1 << 20), "diskBytes": diskCacheMax, "diskGiB": float64(diskCacheMax) / (1 << 30), "prefetchBytes": prefetchBytes, "prefetchMiB": float64(prefetchBytes) / (1 << 20), "prefetchChunks": int((prefetchBytes + chunkSize - 1) / chunkSize), "remoteConcurrency": a.cfg.RemoteConcurrency, "prefetchConcurrency": a.cfg.PrefetchConcurrency}, "current": map[string]interface{}{"memoryBytes": mb, "memoryMiB": float64(mb) / (1 << 20), "memoryEntries": me, "diskBytes": db, "diskMiB": float64(db) / (1 << 20), "diskEntries": de, "inflightChunks": a.inflightCount(), "prefetchJobs": a.prefetchCount()}})
}

func (a *App) root(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)
	_, _ = w.Write(publicIndex)
}

func (a *App) router(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/linkasset/") && (r.Method == "GET" || r.Method == "HEAD") {
		a.serveLinkAsset(w, r, strings.TrimPrefix(r.URL.Path, "/linkasset/"))
		return
	}
	if strings.HasPrefix(r.URL.Path, "/linkplaylist/") && r.Method == "GET" {
		a.serveLinkPlaylist(w, r, strings.TrimPrefix(r.URL.Path, "/linkplaylist/"))
		return
	}
	if r.URL.Path == "/api/upload-url" && r.Method == "POST" {
		a.uploadURLHandler(w, r)
		return
	}
	if r.URL.Path == "/" && r.Method == "GET" {
		a.root(w, r)
		return
	}
	if r.URL.Path == "/files" && r.Method == "GET" {
		a.filesPage(w, r)
		return
	}
	if r.URL.Path == "/api/status" && r.Method == "GET" {
		a.status(w, r)
		return
	}
	if r.URL.Path == "/api/cache" && r.Method == "GET" {
		a.cacheStatus(w, r)
		return
	}
	if r.URL.Path == "/api/files" && r.Method == "GET" {
		a.apiFiles(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/videos/") {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) >= 3 {
			id := parts[2]
			if len(parts) == 3 && r.Method == "GET" {
				a.getVideo(w, r, id)
				return
			}
			if len(parts) == 4 && parts[3] == "complete" && r.Method == "POST" {
				a.complete(w, r, id)
				return
			}
			if len(parts) == 4 && parts[3] == "progress" && r.Method == "GET" {
				a.progress(w, r, id)
				return
			}
			if len(parts) == 5 && parts[3] == "chunks" && r.Method == "POST" {
				idx, _ := strconv.Atoi(parts[4])
				a.uploadChunk(w, r, id, idx)
				return
			}
		}
	}
	if r.URL.Path == "/api/videos" && r.Method == "POST" {
		a.createVideo(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/file/") && (r.Method == "GET" || r.Method == "HEAD") {
		id, _ := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/file/"))
		m, _ := a.getManifest(id)
		if m == nil {
			// Persistent URLs include the original extension (e.g. /file/<id>.mp4).
			// The manifest key itself remains the UUID without the extension.
			base := strings.TrimSuffix(id, filepath.Ext(id))
			if base != id {
				id = base
				m, _ = a.getManifest(id)
			}
		}
		a.handleFile(w, r, m)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/video/") && (r.Method == "GET" || r.Method == "HEAD") {
		id, _ := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/video/"))
		m, _ := a.getManifest(id)
		if m == nil {
			base := strings.TrimSuffix(id, filepath.Ext(id))
			if base != id {
				id = base
				m, _ = a.getManifest(id)
			}
		}
		a.handleFile(w, r, m)
		return
	}
	jsonResp(w, 404, map[string]interface{}{"success": false, "error": "Not Found"})
}

// Link-upload tasks are persisted under storage/linkjobs. Every 360 object has a
// 125-byte PNG prefix; /linkasset strips that prefix and provides the original bytes.
func (a *App) linkDir() string {
	d := filepath.Join(a.cfg.StorageDir, "linkjobs")
	_ = os.MkdirAll(d, 0755)
	return d
}
func (a *App) saveLinkAsset(x LinkAsset) error {
	b, e := json.MarshalIndent(x, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(a.linkDir(), "asset-"+x.ID+".json"), b, 0644)
}
func (a *App) loadLinkAsset(id string) (LinkAsset, error) {
	var x LinkAsset
	b, e := os.ReadFile(filepath.Join(a.linkDir(), "asset-"+filepath.Base(id)+".json"))
	if e == nil {
		e = json.Unmarshal(b, &x)
	}
	return x, e
}
func (a *App) saveLinkPlaylist(x LinkPlaylist) error {
	b, e := json.MarshalIndent(x, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(a.linkDir(), "playlist-"+x.ID+".json"), b, 0644)
}
func (a *App) loadLinkPlaylist(id string) (LinkPlaylist, error) {
	var x LinkPlaylist
	id = strings.TrimSuffix(filepath.Base(id), filepath.Ext(id))
	b, e := os.ReadFile(filepath.Join(a.linkDir(), "playlist-"+id+".json"))
	if e == nil {
		e = json.Unmarshal(b, &x)
	}
	return x, e
}
func (a *App) serveLinkPlaylist(w http.ResponseWriter, r *http.Request, id string) {
	x, e := a.loadLinkPlaylist(id)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	_, _ = io.WriteString(w, x.Content)
}
func (a *App) serveLinkAsset(w http.ResponseWriter, r *http.Request, id string) {
	x, e := a.loadLinkAsset(id)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(x.Size, 10))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", x.ContentType)
		return
	}
	start, end := int64(0), x.Size-1
	partial := false
	if rg := r.Header.Get("Range"); rg != "" {
		var a0, b0 int64
		if _, e := fmt.Sscanf(rg, "bytes=%d-%d", &a0, &b0); e != nil {
			if _, e2 := fmt.Sscanf(rg, "bytes=%d-", &a0); e2 != nil {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", x.Size))
				http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			b0 = x.Size - 1
		}
		if a0 < 0 || a0 >= x.Size {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", x.Size))
			http.Error(w, "range unsatisfiable", 416)
			return
		}
		if b0 < a0 || b0 >= x.Size {
			b0 = x.Size - 1
		}
		start, end, partial = a0, b0, true
	}
	length := end - start + 1
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Type", x.ContentType)
	w.Header().Set("Cache-Control", "public, max-age=3600")
	if partial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, x.Size))
		w.WriteHeader(http.StatusPartialContent)
	}
	for _, part := range x.Parts {
		ps, pe := part.Offset, part.Offset+part.Size-1
		if pe < start || ps > end {
			continue
		}
		req, er := http.NewRequestWithContext(r.Context(), "GET", part.URL, nil)
		if er != nil {
			return
		}
		rr, er := a.client.Do(req)
		if er != nil {
			return
		}
		body, er := io.ReadAll(io.LimitReader(rr.Body, part.Size+126))
		rr.Body.Close()
		if er != nil || rr.StatusCode < 200 || rr.StatusCode >= 300 || len(body) < 125 {
			return
		}
		payload := body[125:]
		lo := maxInt64(start-ps, 0)
		hi := minInt64(end-ps+1, int64(len(payload)))
		if hi > lo {
			_, _ = w.Write(payload[lo:hi])
		}
	}
}
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func (a *App) fetchRemote(ctx context.Context, u *url.URL) ([]byte, *http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if e != nil {
		return nil, nil, e
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, e := a.client.Do(req)
	if e != nil {
		return nil, nil, e
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, resp, fmt.Errorf("source HTTP %d for %s", resp.StatusCode, u.String())
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	resp.Body.Close()
	return b, resp, e
}
func (a *App) uploadLinkAsset(ctx context.Context, filename string, parts []map[string]interface{}, size int64, contentType string) (LinkAsset, error) {
	x := LinkAsset{ID: uuidv4(), Filename: sanitizeFilename(filename), Size: size, CreatedAt: nowISO(), ContentType: contentType}
	for _, p := range parts {
		x.Parts = append(x.Parts, LinkPart{Index: p["index"].(int), Offset: int64(p["offset"].(int64)), Size: int64(p["size"].(int)), URL: p["url"].(string)})
	}
	e := a.saveLinkAsset(x)
	return x, e
}
func (a *App) uploadPlaylistRecursive(ctx context.Context, playlistURL *url.URL, depth int) (string, error) {
	if depth > 5 {
		return "", fmt.Errorf("M3U8 nesting exceeds 5 levels")
	}
	data, _, e := a.fetchRemote(ctx, playlistURL)
	if e != nil {
		return "", e
	}
	textData := strings.TrimSpace(strings.TrimPrefix(strings.Trim(string(bytes.Trim(data, "\x00 \r\n\t")), "\ufeff"), "\ufeff"))
	if !strings.HasPrefix(strings.ToUpper(textData), "#EXTM3U") {
		return "", fmt.Errorf("URL is not an M3U8 playlist: %s", playlistURL)
	}
	lines := strings.Split(strings.ReplaceAll(textData, "\r\n", "\n"), "\n")
	isMaster := false
	for _, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), "#EXT-X-STREAM-INF") {
			isMaster = true
			break
		}
	}
	if isMaster {
		for i, ln := range lines {
			if strings.HasPrefix(strings.TrimSpace(ln), "#EXT-X-STREAM-INF") {
				for j := i + 1; j < len(lines); j++ {
					v := strings.TrimSpace(strings.Trim(lines[j], "\x00"))
					if v == "" || strings.HasPrefix(v, "#") {
						continue
					}
					child, er := playlistURL.Parse(v)
					if er != nil {
						return "", er
					}
					return a.uploadPlaylistRecursive(ctx, child, depth+1)
				}
			}
		}
		return "", fmt.Errorf("master playlist has no variant URI")
	}
	out := make([]string, 0, len(lines))
	uploaded := 0
	for _, ln := range lines {
		t := strings.TrimSpace(strings.Trim(ln, "\x00"))
		if t == "" {
			continue
		}
		if strings.HasPrefix(t, "#") {
			out = append(out, t)
			continue
		}
		ref, er := playlistURL.Parse(t)
		if er != nil {
			return "", er
		}
		segReq, er := http.NewRequestWithContext(ctx, "GET", ref.String(), nil)
		if er != nil {
			return "", er
		}
		segReq.Header.Set("User-Agent", "Mozilla/5.0")
		sr, er := a.client.Do(segReq)
		if er != nil {
			return "", fmt.Errorf("segment fetch failed: %w", er)
		}
		if sr.StatusCode < 200 || sr.StatusCode >= 300 {
			code := sr.StatusCode
			sr.Body.Close()
			return "", fmt.Errorf("segment HTTP %d: %s", code, ref)
		}
		seg, er := io.ReadAll(io.LimitReader(sr.Body, 512<<20))
		sr.Body.Close()
		if er != nil {
			return "", er
		}
		parts, er := a.upload360Parts(ctx, seg, filepath.Base(ref.Path))
		if er != nil {
			return "", er
		}
		id := uuidv4()
		content := "application/octet-stream"
		asset := LinkAsset{ID: id, Filename: filepath.Base(ref.Path), Size: int64(len(seg)), Parts: []LinkPart{}, CreatedAt: nowISO(), ContentType: content}
		for _, p := range parts {
			asset.Parts = append(asset.Parts, LinkPart{Index: p["index"].(int), Offset: int64(p["offset"].(int)), Size: int64(p["size"].(int)), URL: p["url"].(string)})
		}
		if er = a.saveLinkAsset(asset); er != nil {
			return "", er
		}
		out = append(out, "/linkasset/"+id)
		uploaded++
	}
	id := uuidv4()
	content := strings.Join(out, "\n") + "\n"
	pl := LinkPlaylist{ID: id, Filename: filepath.Base(playlistURL.Path), Content: content, CreatedAt: nowISO()}
	if e = a.saveLinkPlaylist(pl); e != nil {
		return "", e
	}
	_ = uploaded
	return "/linkplaylist/" + id + ".m3u8", nil
}

// uploadURLHandler downloads a URL, uploads chunks to 360 and persists a stable
// backend URL. M3U8 master playlists are recursively resolved to media playlists.
func (a *App) uploadURLHandler(w http.ResponseWriter, r *http.Request) {
	var in struct {
		URL string `json:"url"`
	}
	if r.Body == nil {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "missing JSON body"})
		return
	}
	if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); e != nil {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "invalid JSON: " + e.Error()})
		return
	}
	u, e := url.Parse(strings.TrimSpace(in.URL))
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": "url must be http(s)"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Hour)
	defer cancel()
	// Read a bounded prefix for content-type sniffing, then continue from the same stream.
	req, e := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if e != nil {
		jsonResp(w, 400, map[string]interface{}{"success": false, "error": e.Error()})
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, e := a.client.Do(req)
	if e != nil {
		jsonResp(w, 502, map[string]interface{}{"success": false, "error": "source fetch failed: " + e.Error()})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		jsonResp(w, 502, map[string]interface{}{"success": false, "error": fmt.Sprintf("source HTTP %d", resp.StatusCode)})
		return
	}
	br := bufio.NewReaderSize(resp.Body, 4096)
	peek, _ := br.Peek(4096)
	trimPeek := strings.Trim(string(bytes.Trim(peek, "\x00 \r\n\t")), "\ufeff")
	isM3U8 := strings.HasPrefix(strings.ToUpper(trimPeek), "#EXTM3U") || strings.HasSuffix(strings.ToLower(u.Path), ".m3u8") || strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "mpegurl")
	if isM3U8 { // initial response already fetched; recursive fetch is safe and correctly handles master->media playlists
		out, e := a.uploadPlaylistRecursive(ctx, u, 0)
		if e != nil {
			jsonResp(w, 502, map[string]interface{}{"success": false, "type": "m3u8", "error": e.Error()})
			return
		}
		jsonResp(w, 200, map[string]interface{}{"success": true, "type": "m3u8", "filename": filepath.Base(u.Path), "playlistUrl": out, "playable": true, "message": "主/子播放列表已解析，媒体分片已上传并生成后端播放列表"})
		return
	}
	filename := filepath.Base(u.Path)
	if filename == "." || filename == "/" || filename == "" {
		filename = "upload.bin"
	}
	parts, total, upErr := a.upload360Reader(ctx, br, filename)
	// Persist even partial successes, so the returned task can be diagnosed/resumed.
	asset := LinkAsset{ID: uuidv4(), Filename: sanitizeFilename(filename), Size: total, CreatedAt: nowISO(), ContentType: resp.Header.Get("Content-Type")}
	if asset.ContentType == "" {
		asset.ContentType = "application/octet-stream"
	}
	for _, p := range parts {
		asset.Parts = append(asset.Parts, LinkPart{Index: p["index"].(int), Offset: int64(p["offset"].(int64)), Size: int64(p["size"].(int)), URL: p["url"].(string)})
	}
	saveErr := a.saveLinkAsset(asset)
	if upErr != nil {
		jsonResp(w, 502, map[string]interface{}{"success": false, "type": "file", "id": asset.ID, "filename": filename, "size": total, "uploadedParts": parts, "error": upErr.Error(), "manifestSaved": saveErr == nil})
		return
	}
	jsonResp(w, 200, map[string]interface{}{"success": true, "type": "file", "id": asset.ID, "filename": filename, "size": total, "chunkSize": linkUploadChunkSize, "uploadConcurrency": linkUploadWorkers, "parts": parts, "url": "/linkasset/" + asset.ID, "manifestSaved": saveErr == nil})
}

const linkUploadChunkSize = 5 << 20
const linkUploadWorkers = 8

type linkUploadJob struct {
	index  int
	data   []byte
	offset int64
}
type linkUploadResult struct {
	index int
	part  map[string]interface{}
	err   error
}

// upload360Reader reads a remote source incrementally. It retains at most eight
// 5 MiB chunks in flight and starts a maximum of eight uploads concurrently.
// The source is not staged as a complete local file; each chunk is released as
// soon as its upload result has been collected.
func (a *App) upload360Reader(ctx context.Context, src io.Reader, filename string) ([]map[string]interface{}, int64, error) {
	jobs := make(chan linkUploadJob, linkUploadWorkers)
	results := make(chan linkUploadResult, linkUploadWorkers)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < linkUploadWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				name := fmt.Sprintf("%s.part%06d.png", filename, job.index+1)
				link, err := uploadOne360(workerCtx, a.client, job.data, name)
				part := map[string]interface{}{"index": job.index, "offset": job.offset, "size": len(job.data), "url": link}
				select {
				case results <- linkUploadResult{index: job.index, part: part, err: err}:
				case <-workerCtx.Done():
					return
				}
				if err != nil {
					cancel()
					return
				}
			}
		}()
	}
	producerDone := make(chan struct{})
	var total int64
	var readErr error
	go func() {
		defer close(producerDone)
		defer close(jobs)
		buf := make([]byte, linkUploadChunkSize)
		idx := 0
		for {
			n, er := io.ReadFull(src, buf)
			if er == io.EOF {
				return
			}
			if er != nil && er != io.ErrUnexpectedEOF {
				readErr = er
				cancel()
				return
			}
			if n == 0 {
				return
			}
			piece := make([]byte, n)
			copy(piece, buf[:n])
			select {
			case jobs <- linkUploadJob{index: idx, data: piece, offset: total}:
				total += int64(n)
				idx++
			case <-workerCtx.Done():
				return
			}
			if er == io.ErrUnexpectedEOF {
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	parts := make([]map[string]interface{}, 0)
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("360 part %d upload failed: %w", res.index, res.err)
			}
			continue
		}
		parts = append(parts, res.part)
	}
	<-producerDone
	if readErr != nil && firstErr == nil {
		firstErr = fmt.Errorf("source read failed: %w", readErr)
	}
	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i]["index"].(int) < parts[j]["index"].(int) })
	if firstErr != nil {
		return parts, total, firstErr
	}
	return parts, total, nil
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func (a *App) upload360Parts(ctx context.Context, data []byte, filename string) ([]map[string]interface{}, error) {
	if filename == "" || filename == "." || filename == "/" {
		filename = "upload.bin"
	}
	const maxPart = 5 << 20
	out := make([]map[string]interface{}, 0, (len(data)+maxPart-1)/maxPart)
	for off := 0; off < len(data); off += maxPart {
		end := off + maxPart
		if end > len(data) {
			end = len(data)
		}
		part := data[off:end]
		name := fmt.Sprintf("%s.part%04d.png", filename, len(out)+1)
		link, err := uploadOne360(ctx, a.client, part, name)
		if err != nil {
			return out, err
		}
		out = append(out, map[string]interface{}{"index": len(out), "offset": off, "size": len(part), "url": link})
	}
	return out, nil
}

// makePNGPrefix125 returns an exactly 125-byte standards-compliant 1x1 PNG.
// A tEXt ancillary chunk is inserted before IEND to reach the required length.
func makePNGPrefix125() ([]byte, error) {
	img := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.NRGBA{R: 0, G: 0, B: 0, A: 0})
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, img); err != nil {
		return nil, err
	}
	base := encoded.Bytes()
	if len(base) < 20 || string(base[len(base)-8:len(base)-4]) != "IEND" {
		return nil, fmt.Errorf("unexpected PNG encoder output")
	}
	// PNG IEND is 12 bytes. The inserted tEXt chunk must make the prefix 125 bytes.
	textDataLen := 125 - len(base) - 12
	if textDataLen < len("Comment\x00") {
		return nil, fmt.Errorf("base PNG too large for 125-byte prefix: %d", len(base))
	}
	textData := make([]byte, textDataLen)
	copy(textData, []byte("Comment\x00"))
	for i := len("Comment\x00"); i < len(textData); i++ {
		textData[i] = 'x'
	}
	chunk := make([]byte, 8+len(textData)+4)
	binary.BigEndian.PutUint32(chunk[:4], uint32(len(textData)))
	copy(chunk[4:8], "tEXt")
	copy(chunk[8:], textData)
	crc := crc32.ChecksumIEEE(chunk[4 : 8+len(textData)])
	binary.BigEndian.PutUint32(chunk[8+len(textData):], crc)
	out := make([]byte, 0, 125)
	out = append(out, base[:len(base)-12]...)
	out = append(out, chunk...)
	out = append(out, base[len(base)-12:]...)
	if len(out) != 125 {
		return nil, fmt.Errorf("prefix length %d, expected 125", len(out))
	}
	if _, err := png.Decode(bytes.NewReader(out)); err != nil {
		return nil, fmt.Errorf("generated PNG prefix is invalid: %w", err)
	}
	return out, nil
}

func uploadOne360(ctx context.Context, client *http.Client, payload []byte, filename string) (string, error) {
	tr := &http.Transport{}
	c := *client
	c.Transport = tr
	req, err := http.NewRequestWithContext(ctx, "GET", "https://console.e.360.cn/api/v1/UploadToken", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Origin", "https://e.360.cn")
	req.Header.Set("Referer", "https://e.360.cn/")
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	var tok struct {
		Code int    `json:"code"`
		Data string `json:"data"`
		Msg  string `json:"msg"`
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tok)
	resp.Body.Close()
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || tok.Code != 200 || tok.Data == "" {
		return "", fmt.Errorf("360 token rejected: HTTP %d code %d %s", resp.StatusCode, tok.Code, tok.Msg)
	}
	// The first 125 bytes must be a valid 1x1 PNG, not a PNG signature followed
	// by random bytes. The original chunk payload starts at byte 125 and is
	// preserved verbatim after the PNG prefix.
	prefix, err := makePNGPrefix125()
	if err != nil {
		return "", fmt.Errorf("build 125-byte PNG prefix: %w", err)
	}
	body := make([]byte, len(prefix)+len(payload))
	copy(body, prefix)
	copy(body[len(prefix):], payload)
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	_ = mw.WriteField("token", tok.Data)
	_ = mw.WriteField("picasso", "1")
	fh := make(textproto.MIMEHeader)
	fh.Set("Content-Disposition", fmt.Sprintf(`form-data; name="uploadfile"; filename="%s"`, strings.ReplaceAll(filename, `"`, "")))
	fh.Set("Content-Type", "image/png")
	fw, err := mw.CreatePart(fh)
	if err != nil {
		return "", err
	}
	if _, err = fw.Write(body); err != nil {
		return "", err
	}
	mw.Close()
	req, err = http.NewRequestWithContext(ctx, "POST", "https://up2.e.360.cn/up", &b)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", "https://e.360.cn")
	req.Header.Set("Referer", "https://e.360.cn/")
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err = c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		Errno int    `json:"errno"`
		Msg   string `json:"msg"`
		Data  struct {
			AppURL string `json:"app_url"`
		} `json:"data"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&result); err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || result.Errno != 0 || result.Data.AppURL == "" {
		return "", fmt.Errorf("360 upload failed: HTTP %d errno %d %s", resp.StatusCode, result.Errno, result.Msg)
	}
	return result.Data.AppURL, nil
}

func main() {
	loadDotEnv()
	cfg := loadConfig()
	runtime.GOMAXPROCS(4)
	debug.SetGCPercent(100)
	a := newApp(cfg)
	if err := a.init(); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", a.router)
	srv := &http.Server{Addr: ": " + cfg.Port, Handler: withPerfMiddleware(mux), ReadTimeout: 0, ReadHeaderTimeout: 30 * time.Second, WriteTimeout: 0, IdleTimeout: 75 * time.Second, MaxHeaderBytes: 64 << 10}
	srv.Addr = "0.0.0.0:" + cfg.Port
	log.Printf("==========================================")
	log.Printf(" Go Universal File Streaming Server | 4C4G")
	log.Printf(" Addr: %s | GOMAXPROCS=%d", srv.Addr, runtime.GOMAXPROCS(0))
	log.Printf(" Chunk: %.1f MiB | Memory cache: %.1f GiB | Disk cache: %.1f GiB", float64(chunkSize)/(1<<20), float64(memoryCacheMax)/(1<<30), float64(diskCacheMax)/(1<<30))
	log.Printf(" Remote concurrency: %d (foreground %d + prefetch %d)", cfg.RemoteConcurrency, cap(a.foregroundSem), cap(a.prefetchSem))
	log.Printf(" Upload concurrency: %d | Prefetch: %d MiB", cfg.UploadConcurrency, int(prefetchBytes)/(1<<20))
	log.Printf("==========================================")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func withPerfMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Powered-By", "")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
