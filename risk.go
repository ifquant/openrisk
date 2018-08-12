package main

import (
	"github.com/thoas/go-funk"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

var predefinedGroupName = []string{
	"sector",
	"industry",
	"subindustry",
	"market",
	"type",
	"currency",
	"acc",
}

const (
	GROUP_SECTOR      = 0
	GROUP_INDUSTRY    = 1
	GROUP_SUBINDUSTRY = 2
	GROUP_MARKET      = 3
	GROUP_TYPE        = 4
	GROUP_CURRENCY    = 5
	GROUP_ACC         = 6
)

type WindowDef struct {
	Seconds int
	Type    string
}

type NameExpression struct {
	Name string
	E    *Expression
}

type RiskParamDef struct {
	Parent     *RiskDef
	Name       string
	Formula    *Expression
	UpperBound float64
	LowerBound float64
	Window     WindowDef
	Variables  []NameExpression
	Graph      bool
	History    map[string][][2]float64 // only if Graph = true
}

type RiskDef struct {
	Path        string // python module path
	Name        string
	Groups      []interface{}
	GroupNames  []string
	Params      []*RiskParamDef
	DisplayName string
	Filter      *Expression
}

func split(s string, pattern string) []string {
	res := strings.FieldsFunc(s, func(r rune) bool {
		return strings.Index(pattern, string(r)) >= 0
	})
	var res2 []string
	for _, s := range res {
		res2 = append(res2, strings.Trim(s, " \t\r"))
	}
	return res2
}

func newRiskParamDef(s *IniSection, parent *RiskDef) (r *RiskParamDef, eres error) {
	f := s.ValueMap["formula"]
	r = &RiskParamDef{
		Parent:     parent,
		Name:       s.Name,
		UpperBound: math.NaN(),
		LowerBound: math.NaN(),
	}
	var params map[string]interface{}
	variables := s.SectionMap["var"]
	if variables != nil {
		params = make(map[string]interface{}, 60)
		for _, nameExpr := range variables.Values {
			res, err := ParseExpr(nameExpr[2], nameExpr[1], "variable", params, nil, parent.Path)
			if err != nil {
				eres = err
				return
			}
			r.Variables = append(r.Variables, NameExpression{nameExpr[0], res})
			params[nameExpr[0]] = 0.0
		}
	}
	if f[0] != "" {
		res, err := ParseExpr(f[1], f[0], "formula", params, nil, parent.Path)
		if err != nil {
			eres = err
			return
		}
		r.Formula = res
	}
	w := split(s.ValueMap["window"][0], ",")
	if len(w) > 0 {
		if v, err := strconv.Atoi(w[0]); err == nil {
			r.Window.Seconds = v
		}
	}
	if len(w) > 1 {
		r.Window.Type = w[1]
	}
	str := s.ValueMap["upper_bound"][0]
	if str != "" {
		if v, err := strconv.ParseFloat(str, 64); err == nil {
			r.UpperBound = v
		}
	}
	str = s.ValueMap["lower_bound"][0]
	if str != "" {
		if v, err := strconv.ParseFloat(str, 64); err == nil {
			r.LowerBound = v
		}
	}
	str = strings.ToLower(s.ValueMap["graph"][0])
	if str == "true" || str == "y" || str == "yes" || str == "1" {
		if r.Formula.A == "" {
			log.Print("Graph only allowable for aggregate formula")
		} else {
			r.Graph = true
			r.History = make(map[string][][2]float64)
		}
	}
	return
}

func newRiskDef(s *IniSection, path string) (r *RiskDef, eres error) {
	r = &RiskDef{
		Path:        path,
		Name:        s.Name,
		GroupNames:  split(s.ValueMap["group_name"][0], ","),
		DisplayName: s.ValueMap["name"][0],
	}
	if r.DisplayName == "" {
		r.DisplayName = r.Name
	}
	tmp := s.ValueMap["group"]
	groups := split(tmp[0], ",")
	for i, g := range groups {
		ig := funk.IndexOf(predefinedGroupName, g)
		if ig < 0 {
			if g == "*" {
				g = "true"
			}
			res, err := ParseExpr(tmp[1], g, "group", nil, true, path)
			if err != nil {
				eres = err
				return
			}
			r.Groups = append(r.Groups, res)
		} else {
			r.Groups = append(r.Groups, ig)
		}
		if i >= len(r.GroupNames) {
			r.GroupNames = append(r.GroupNames, g)
		}
	}
	f := s.ValueMap["f"]
	if f[0] != "" {
		res, err := ParseExpr(f[1], f[0], "filter", nil, true, r.Path)
		if err != nil {
			eres = err
			return
		}
		r.Filter = res
	}
	for _, p := range s.Sections {
		if p.Name == "var" {
			continue
		}
		rp, err := newRiskParamDef(p, r)
		if err != nil {
			eres = err
			return
		}
		r.Params = append(r.Params, rp)
	}
	rp, err := newRiskParamDef(s, r)
	if err != nil {
		eres = err
		return
	}
	if rp.Formula != nil {
		r.Params = append([]*RiskParamDef{rp}, r.Params...)
	}
	return
}

func (self *RiskDef) Run(positions []*Position) interface{} {
	grouped := make(map[string][]*Position)
	if len(self.Groups) > 0 {
		for i, expr := range self.Groups {
			e, eok := expr.(*Expression)
			for _, p := range positions {
				if self.Filter != nil {
					v, _ := Evaluate(self.Filter, p)
					if v2, ok2 := v.(bool); ok2 {
						if !v2 {
							continue
						}
					}
				}
				tmp := ""
				if eok {
					v, _ := Evaluate(e, p)
					if v2, ok2 := v.(bool); ok2 {
						if v2 {
							tmp = self.GroupNames[i]
						}
					}
				} else {
					switch expr {
					case GROUP_SECTOR:
						tmp = p.Security.Sector
					case GROUP_INDUSTRY:
						tmp = p.Security.Industry
					case GROUP_SUBINDUSTRY:
						tmp = p.Security.SubIndustry
					case GROUP_MARKET:
						tmp = p.Security.Market
					case GROUP_TYPE:
						tmp = p.Security.Type
					case GROUP_CURRENCY:
						tmp = p.Security.Currency
					case GROUP_ACC:
						tmp = AccNames[p.Acc]
					}
				}
				if tmp != "" {
					grouped[tmp] = append(grouped[tmp], p)
				}
			}
			if len(self.GroupNames) > i {
				expr = self.GroupNames[i]
			}
		}
	} else {
		grouped[""] = positions
	}
	rpt := make(map[string]interface{})
	for _, rp := range self.Params {
		var out []interface{}
		for gname, positions := range grouped {
			if len(positions) > 0 {
				out = append(out, []interface{}{gname, rp.Run(gname, positions)})
			}
		}
		if len(out) > 0 {
			if len(self.Params) == 1 {
				return out
			} else {
				rpt[rp.Name] = out
			}
		}
	}
	if len(rpt) > 0 {
		return rpt
	}
	return nil
}

func length(nums []float64) float64 {
	return float64(len(nums))
}

func sum(nums []float64) float64 {
	res := 0.
	for _, tmp := range nums {
		res += tmp
	}
	return res
}

func mean(nums []float64) float64 {
	if len(nums) == 0 {
		return math.NaN()
	}
	return sum(nums) / float64(len(nums))
}

func std(nums []float64) float64 {
	// std = sqrt(mean(abs(x - x.mean())**2))
	if len(nums) == 0 {
		return math.NaN()
	}
	mn := mean(nums)
	sd := 0.
	for _, tmp := range nums {
		sd += math.Pow(tmp-mn, 2)
	}
	sd = math.Sqrt(sd / float64(len(nums)))
	return sd
}

func (self *RiskParamDef) evaluate(positions []*Position, params map[string]interface{}, optional ...*Expression) interface{} {
	var e *Expression
	var isFormula bool
	if len(optional) > 0 {
		e = optional[0]
	} else {
		e = self.Formula
		isFormula = true
		if e.A == "" {
			// by default, only return top 10 result
			e.A = "top"
			e.N = 10
		}
	}
	if e.A == "call" {
		res, _ := CallPy(e.C[0], e.C[1], e.C[2], positions, self.Parent.Path)
		return res
	}
	value := math.NaN()
	res := make([]float64, 0, len(positions))
	for _, p := range positions {
		if isFormula {
			// prepare non-aggregate variable
			for _, v := range self.Variables {
				if v.E.A == "" {
					params[v.Name], _ = Evaluate(v.E, p, params)
				}
			}
		}
		tmp, _ := Evaluate(e, p, params)
		res = append(res, tmp.(float64))
	}
	if e.A == "std" {
		value = std(res)
	} else if e.A == "sum" {
		value = sum(res)
	} else if e.A == "len" {
		value = length(res)
	} else if e.A == "mean" {
		value = mean(res)
	} else if e.A == "top" {
		tmp := make([][2]interface{}, 0, len(Positions))
		for i, p := range positions {
			if math.IsNaN(res[i]) {
				continue
			}
			tmp = append(tmp, [2]interface{}{p.Security.Symbol, res[i]})
		}
		// will optimize with nth_element
		n := 0
		if e.N > 0 {
			sort.Slice(tmp, func(i, j int) bool { return tmp[i][1].(float64) > tmp[j][1].(float64) })
			n = e.N
		} else if e.N < 0 {
			sort.Slice(tmp, func(i, j int) bool { return tmp[i][1].(float64) < tmp[j][1].(float64) })
			n = -e.N
		}
		if n < len(tmp) {
			tmp = tmp[:n]
		}
		return tmp
	}
	if math.IsNaN(value) {
		// json Marshal failed to work with NaN, so change to string
		return "NaN"
	}
	return value
}

func (self *RiskParamDef) Run(gname string, positions []*Position) interface{} {
	var params map[string]interface{}
	// prepare aggregate variable
	if len(self.Variables) > 0 {
		params = make(map[string]interface{}, 60)
		for _, v := range self.Variables {
			if v.E.A != "" {
				params[v.Name] = self.evaluate(positions, params, v.E)
			}
		}
	}
	v := self.evaluate(positions, params)
	if self.Graph {
		if v2, ok2 := v.(float64); ok2 {
			tmp := self.History[gname]
			n := len(tmp)
			now := float64(time.Now().Unix())
			if n > 1 && now-tmp[0][0] > 25*3600 { // reduce history every 1h
				for i := 1; i < n; i += 1 {
					if now-tmp[i][0] < 24*3600 {
						tmp = tmp[i:]
						n = len(tmp)
						break
					}
				}
			}
			if n > 1 {
				tmp1 := tmp[n-2]
				tmp2 := tmp[n-1]
				if now-tmp1[0] > 60. && math.Abs(tmp1[1]-v2) > math.Abs(tmp1[1]+v2)/2000. {
					self.History[gname] = append(tmp, [2]float64{now, v2})
				} else {
					tmp2[0] = now
					tmp2[1] = v2
				}
			} else {
				self.History[gname] = append(tmp, [2]float64{now, v2})
			}
		}
	}
	return v
}
