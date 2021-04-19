package tileharvest

import "fmt"

type TileInfo struct {
	Spec  TileSpec  `json:"spec"`
	Stats TileStats `json:"stats"`
}

func (info TileInfo) isNilObj() bool {
	return info.Spec.isNilObj() || info.Stats.isNilObj()
}

func (info TileInfo) makeKey() string {
	return info.Spec.makeKey()
}

type TileSpec struct {
	Zoom int `json:"z"`
	X    int `json:"x"`
	Y    int `json:"y"`
}

func (spec TileSpec) isNilObj() bool {
	return spec.Zoom == 0 && spec.Y == 0 && spec.X == 0
}

func (spec TileSpec) makeKey() string {
	return fmt.Sprintf("%d-%d-%d", spec.Zoom, spec.X, spec.Y)
}

type TileStats struct {
	CPUPercent          int  `json:"cpu_percent"`
	WallTimeSeconds     int `json:"wall_time_seconds"`
	MaxResidentKilobytes int `json:"max_resident_kb"`
}

func (stats TileStats) isNilObj() bool {
	return stats.CPUPercent == 0 && stats.MaxResidentKilobytes == 0 && stats.WallTimeSeconds == 0
}
