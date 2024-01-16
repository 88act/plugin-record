package record

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	. "m7s.live/engine/v4"
	"m7s.live/engine/v4/codec"
	"m7s.live/engine/v4/codec/mpegts"
	"m7s.live/engine/v4/util"
	"m7s.live/plugin/hls/v4"
)

type HLSRecorder struct {
	playlist           hls.Playlist
	tsStartTime        uint32
	tsLastTime         uint32
	tsTitle            string
	video_cc, audio_cc byte
	Recorder
	MemoryTs
}

func NewHLSRecorder() (r *HLSRecorder) {
	r = &HLSRecorder{}
	r.Record = RecordPluginConfig.Hls
	return r
}

func (h *HLSRecorder) Start(streamPath string) error {
	h.ID = streamPath + "/hls"
	fmt.Println("开始------Start -", h.ID)
	fmt.Println("开始------SUBTYPE_RAW -", SUBTYPE_RAW)
	return h.start(h, streamPath, SUBTYPE_RAW)
}
func (r *HLSRecorder) Close() (err error) {
	if r.File != nil {

		inf := hls.PlaylistInf{
			Duration: float64(r.tsLastTime-r.tsStartTime) / 1000,
			Title:    r.tsTitle,
		}
		fmt.Println("停止录制 r.Stream.Path  === ", r.Stream.Path)
		pathName := strings.Split(r.Stream.Path, "/")
		if len(pathName) > 0 {
			inf.Title = pathName[len(pathName)-1] + "/" + inf.Title
		}
		fmt.Println("停止录制======>Duration =%s, FilePath =%s, Title =%s ", inf.Duration, inf.FilePath, inf.Title)
		r.playlist.WriteInf(inf)
		r.tsStartTime = 0
		err = r.File.Close()
	}
	return
}
func (h *HLSRecorder) OnEvent(event any) {
	var err error
	defer func() {
		if err != nil {
			h.Stop(zap.Error(err))
		}
	}()
	switch v := event.(type) {
	case *HLSRecorder:
		h.BytesPool = make(util.BytesPool, 17)
		fmt.Println("===== HLS Recorder) OnEvent======>CreateFile ===   ", h.Recorder.filePath, h.Recorder.Ext)
		if h.Writer, err = h.Recorder.CreateFileM3u8(); err != nil {
			return
		}
		h.SetIO(h.Writer)
		h.playlist = hls.Playlist{
			Writer:         h.Writer,
			Version:        3,
			Sequence:       0,
			Targetduration: int(math.Ceil(h.Fragment.Seconds())),
		}
		if err = h.playlist.Init(); err != nil {
			return
		}
		if h.File, err = h.CreateFile(); err != nil {
			return
		}
	case AudioFrame:
		if h.tsStartTime == 0 {
			h.tsStartTime = v.AbsTime
		}
		h.tsLastTime = v.AbsTime
		h.Recorder.OnEvent(event)
		pes := &mpegts.MpegtsPESFrame{
			Pid:                       mpegts.PID_AUDIO,
			IsKeyFrame:                false,
			ContinuityCounter:         h.audio_cc,
			ProgramClockReferenceBase: uint64(v.DTS),
		}
		h.WriteAudioFrame(v, pes)
		_, err = h.BLL.WriteTo(h.File)
		h.Recycle()
		h.Clear()
		h.audio_cc = pes.ContinuityCounter
	case VideoFrame:
		if h.tsStartTime == 0 {
			h.tsStartTime = v.AbsTime
		}
		h.tsLastTime = v.AbsTime
		h.Recorder.OnEvent(event)
		pes := &mpegts.MpegtsPESFrame{
			Pid:                       mpegts.PID_VIDEO,
			IsKeyFrame:                v.IFrame,
			ContinuityCounter:         h.video_cc,
			ProgramClockReferenceBase: uint64(v.DTS),
		}
		if err = h.WriteVideoFrame(v, pes); err != nil {
			return
		}
		_, err = h.BLL.WriteTo(h.File)
		h.Recycle()
		h.Clear()
		h.video_cc = pes.ContinuityCounter
	default:
		h.Recorder.OnEvent(v)
	}
}

// 创建一个新的ts文件
func (h *HLSRecorder) CreateFile() (fw FileWr, err error) {
	h.tsTitle = fmt.Sprintf("%d.ts", time.Now().Unix())
	filePath := filepath.Join(h.Stream.Path, h.tsTitle)
	fmt.Println("创建一个新的ts文件 , h.tsTitle = ", h.tsTitle)
	fmt.Println("创建一个新的ts文件 , h.Stream.Path= ", h.Stream.Path)
	fw, err = h.CreateFileFn(filePath, false)
	if err != nil {
		h.Error("create file", zap.String("path", filePath), zap.Error(err))
		return
	}
	h.Info("create file", zap.String("path", filePath))

	if err = mpegts.WriteDefaultPATPacket(fw); err != nil {
		return
	}
	var vcodec codec.VideoCodecID = 0
	var acodec codec.AudioCodecID = 0
	if h.Video != nil {
		vcodec = h.Video.CodecID
	}
	if h.Audio != nil {
		acodec = h.Audio.CodecID
	}
	mpegts.WritePMTPacket(fw, vcodec, acodec)
	return
}
