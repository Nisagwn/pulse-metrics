// Package buildinfo: ikili dosyanin kim oldugunu soyler.
//
// Degerler derleme sirasinda -ldflags ile doldurulur:
//
//	go build -ldflags "-X github.com/nisah/pulse-metrics/internal/buildinfo.Version=v1.0.0"
//
// Neden onemli? Uretimde bir sorun cikinca sorulacak ilk soru "hangi surum
// calisiyor?" olur. /healthz'in "week1" yazan sabit bir surum alani
// dondurmesi bu soruya cevap vermiyordu. Simdi hem saglik ucundan hem de
// Prometheus'taki pulse_build_info olcusunden okunabiliyor.
package buildinfo

import (
	"runtime"
	"runtime/debug"
)

// Bu uc degisken -ldflags ile disaridan atanir. Atanmazlarsa asagidaki
// varsayilanlar gecerli olur; "dev" gormek "bu bir gelistirme derlemesi"
// demektir, bilgi eksikligi degil.
var (
	Version   = "dev"
	Commit    = ""
	BuildTime = ""
)

// Info: ikili dosya hakkinda toplanabilen her sey.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

// Get: ldflags ile verilen degerleri, verilmediyse Go'nun kendi gomdugu
// VCS bilgisiyle tamamlar.
//
// debug.ReadBuildInfo, git deposundan derlenen ikili dosyalara Go'nun
// otomatik gomdugu vcs.revision/vcs.time anahtarlarini icerir. Yani
// -ldflags unutulsa bile commit bilgisi cogu zaman elimizde olur.
func Get() Info {
	info := Info{
		Version:   Version,
		Commit:    Commit,
		BuildTime: BuildTime,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}

	if info.Commit == "" || info.BuildTime == "" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					if info.Commit == "" {
						info.Commit = shorten(s.Value)
					}
				case "vcs.time":
					if info.BuildTime == "" {
						info.BuildTime = s.Value
					}
				}
			}
		}
	}
	return info
}

// String: log satirlarina sigacak tek satirlik ozet.
func (i Info) String() string {
	s := i.Version
	if i.Commit != "" {
		s += " (" + i.Commit + ")"
	}
	return s + " " + i.GoVersion + " " + i.Platform
}

func shorten(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}
