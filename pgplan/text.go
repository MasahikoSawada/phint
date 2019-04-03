package pgplan

import (
	"github.com/golang/glog"
)

func GetPlanFromText(planTxtStr string) *Plan {
	glog.Error("parsing text formatted query plan is not supported yet")
	return new(Plan)
}
