package pgplan

import (
	"fmt"
	"strings"
	//"github.com/golang/glog"
)

func GetPlanFromText(planTxtStr []string) *Plan {
	p := new(Plan)

	for _, line := range planTxtStr {

		idx := strings.Index(line, "-> ")

		if idx == -1 {
			continue
		}

		fmt.Println(line)
	}

	return p
}
