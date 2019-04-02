package pgplan

import (
	"encoding/json"
	"github.com/golang/glog"
)

// Parse one plan string in json format and return the parsed Plan obects
func GetPlanFromJson(planJsonStr string) *Plan {
	p := new(Plan)

	//fmt.Println(planJsonStr)
	planJsonByte := ([]byte)(planJsonStr)
	if err := json.Unmarshal(planJsonByte, &p.Branches); err != nil {
		glog.Fatal(err)
	}

	p.PlanStr = planJsonStr
	return p
}
