package pgplan

import (
	"fmt"
	"github.com/golang/glog"
)

// Struct for one PostgreSQL plan
type Plan struct {
	Branches []PlanBranch
	PlanStr  string
}

type PlanBranch struct {
	Root Node `json:"Plan"`
}

// Struct for one Node.
//
// Please note that this includes some fields that we don't use yet.
type Node struct {
	NodeType          string  `json:"Node Type"`
	JoinType          string  `json:"Join Type"`
	RelationName      string  `json:"Relation Name"`
	IndexName         string  `json:"Index Name"`
	CTEName           string  `json:"CTE Name"`
	Alias             string  `json:"Alias"`
	InnerUnique       bool    `json:"Inner Unique"`
	StartupCost       float64 `json:"Startup Cost"`
	TotalCost         float64 `json:"Total Cost"`
	PlanRows          float64 `json:"Plan Rows"`
	PlanWidth         float64 `json:"Plan Width"`
	ParentRelatioship string  `json:"Parent Relationship"`
	ParallelAware     bool    `json:"Paralell Aware"`
	SubplanName       string  `json:"Subplan Name"`
	ActualStartupTime float64 `json:"Actual Startup Time"`
	ActualRows        float64 `json:"Actual Rows"`
	ActualLoops       float64 `json:"Actual Loops"`

	// Subplans
	Childs []Node `json:"Plans"`
}

type Hint struct {
	// Planner hints
	Leading string
	Scan    []string
	Join    []string
	Rows    string // not supported yet

	// original plan
	OrigPlan *Plan
}

// Get node type string after removed whitespaces
func (n *Node) getHintKeyword() string {
	var hint string
	switch n.NodeType {
	case "Nested Loop":
		hint = "NestLoop"
	case "Hash Join":
		hint = "HashJoin"
	case "Merge Join":
		hint = "MergeJoin"
	case "Seq Scan":
		hint = "SeqScan"
	case "Index Scan":
		hint = "IndexScan"
	case "Index Only Scan":
		hint = "IndexOnlyScan"
	case "Bitmap Heap Scan":
		hint = "BitmapScan"
	case "CTE Scan":
		hint = "CTEScan"
	case "Sample Scan":
		hint = "SampleScan"
	default:
		glog.Error("please define new Node type: " + n.NodeType)
	}
	return hint
}

// Add relation to the tail of each Join hints
func addJoinRel(h *Hint, relname string) {
	for i := 0; i < len(h.Join); i++ {
		j := &(h.Join[i])
		len := len(*j)
		sp := ""

		if (*j)[len-1] != '(' {
			// is first relation?
			sp = " "
		}
		if (*j)[len-1] != ')' {
			// append it if the join hint has not closed yet
			h.Join[i] += sp + relname
		}
	}
}

func (n *Node) getChildIdxName() string {
	var indName string
	c := n.Childs[0]

	if c.NodeType == "BitmapOr" || c.NodeType == "BitmapAnd" {
		// BitmapOr/And could has multiple childs nodes of BitMapIndexScan and each
		// of them could have different index name. Collect them first and then eliminate
		// duplication, finally create string having unique index names.
		var indexNameList []string
		sp := ""

		m := make(map[string]struct{})

		for _, cc := range c.Childs {
			indexNameList = append(indexNameList, cc.IndexName)
		}

		indexNameUniqueList := make([]string, 0)

		// Duplicate elimination for index names
		for _, element := range indexNameList {

			if _, ok := m[element]; !ok {
				m[element] = struct{}{}
				indexNameUniqueList = append(indexNameUniqueList, element)
			}
		}

		for _, ind := range indexNameUniqueList {
			indName += sp + ind
			sp = " "
		}
	} else {
		indName = c.IndexName
	}

	return indName
}

func getHintFromNode(n Node, h *Hint) {

	switch n.NodeType {
	case "Nested Loop", "Hash Join", "Merge Join":
		// Leading
		h.Leading += "("

		// Join
		h.Join = append(h.Join, n.getHintKeyword()+"(")
		myjoinPos := len(h.Join) - 1

		sp := ""

		for _, c := range n.Childs {
			// Leading
			h.Leading += sp

			getHintFromNode(c, h)

			sp = " "

		}
		// Leading
		h.Leading += ")"

		// Join
		h.Join[myjoinPos] += ")"
	case "CTE Scan":
		// Leading
		h.Leading += n.CTEName

		// No Scan

		// Join
		addJoinRel(h, n.CTEName)
	case "Bitmap Heap Scan":
		// Leading
		h.Leading += n.RelationName

		// Scan
		h.Scan = append(h.Scan,
			n.getHintKeyword()+"("+n.RelationName+" "+n.getChildIdxName()+")")

		// Join
		addJoinRel(h, n.RelationName)
	case "Seq Scan", "Index Scan", "Index Only Scan":
		// Leading
		h.Leading += n.RelationName

		// Scan
		if n.NodeType == "Index Scan" {
			h.Scan = append(h.Scan, n.getHintKeyword()+"("+n.RelationName+" "+n.IndexName+")")
		} else {
			h.Scan = append(h.Scan, n.getHintKeyword()+"("+n.RelationName+")")
		}

		// Join
		addJoinRel(h, n.RelationName)
	case "Materialize", "Sort", "Hash", "Bitmap Index Scan", "BitmapOr", "BitmapAnd", "Result", "Aggregate", "Hash Aggregate":
		getHintFromNode(n.Childs[0], h)
	default:
		glog.Info("might be better to add the new Node for parsing: " + n.NodeType)
	}
}

func getHintFromBranch(b PlanBranch, h *Hint) {
	getHintFromNode(b.Root, h)
}

// Get planner hint from one PostgreSQL plan.
// PostgreSQL plan could have multiple PlanBranches.
func GetHintFromPlan(plan *Plan) *Hint {
	hint := new(Hint)

	hint.Leading = "Leading("
	hint.OrigPlan = plan

	for _, b := range plan.Branches {
		getHintFromBranch(b, hint)
	}

	hint.Leading += ")"

	return hint
}

// Return string representation planner hints
func (h *Hint) GetAllHints() string {
	allHints := "/*+\n"
	allHints += h.Leading + "\n"
	for _, j := range h.Join {
		allHints += j + "\n"
	}
	for _, s := range h.Scan {
		allHints += s + "\n"
	}
	allHints += "*/"

	return allHints
}

// Dump contents of a plan
func (n *Node) dump(level int) {
	var indent string

	for i := 0; i < level; i++ {
		indent += "  "
	}

	fmt.Println(indent, "-", "Node Type: ", n.NodeType)
	indent += "  "
	fmt.Println(indent, "Relation Name: ", n.RelationName)
	fmt.Println(indent, "Plan Rows: ", n.PlanRows)
	fmt.Println(indent, "Actual Rows: ", n.ActualRows)

	for _, child := range n.Childs {
		child.dump(level + 1)
	}
}

func (p *PlanBranch) dump() {
	p.Root.dump(0)
}

// Dump one plan
func (p *Plan) dump() {
	for _, plan := range p.Branches {
		plan.dump()
	}
}
