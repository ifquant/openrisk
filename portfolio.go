package main

import (
	"github.com/thoas/go-funk"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Portfolio struct {
	Name        string
	RiskDefs    []*RiskDef
	AccPatterns string
	Filter      *Expression
}

func ParsePortfolio(cfg *IniSection, path string) (p *Portfolio, eres error) {
	p = &Portfolio{
		Name:        cfg.ValueMap["name"][0],
		AccPatterns: cfg.ValueMap["acc"][0],
	}
	for _, r := range cfg.Sections {
		rd, err := newRiskDef(r, path)
		if err != nil {
			eres = err
			return
		}
		p.RiskDefs = append(p.RiskDefs, rd)
	}
	f := cfg.ValueMap["filter"]
	if f[0] != "" {
		res, err := ParseExpr(f[1], f[0], "filter", nil, true, path)
		if err != nil {
			eres = err
			return
		}
		p.Filter = res
	}
	return
}

var UserIdAccs = make(map[int][]int)
var AccNames = make(map[int]string)

func ParseUserIdAcc(msg []interface{}) {
	userId := int(msg[1].(float64))
	acc := int(msg[2].(float64))
	accName := msg[3].(string)
	AccNames[acc] = accName
	action := ""
	if len(msg) > 4 {
		action = msg[4].(string)
	}
	i := funk.IndexOf(UserIdAccs[userId], acc)
	tmp := UserIdAccs[userId]
	if action == "delete" {
		if i >= 0 && len(tmp) > 0 {
			UserIdAccs[userId] = append(tmp[:i], tmp[i+1:]...)
		}
	} else {
		if i < 0 {
			UserIdAccs[userId] = append(tmp, acc)
		}
	}
	parsePortfolios(userId)
}

func (p *Portfolio) Run(positions []*Position) map[string]interface{} {
	rpt := make(map[string]interface{})
	for _, riskDef := range p.RiskDefs {
		name := riskDef.DisplayName
		tmp := riskDef.Run(positions)
		if tmp != nil {
			rpt[name] = tmp
		}
	}
	return rpt
}

func copy(from string, to string) error {
	data, err := ioutil.ReadFile(from)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(to, data, 0755)
	return err
}

var UserPortfolios = make(map[int]map[string]*Portfolio)

func GetPath(userId int) string {
	return "__" + strconv.Itoa(userId) + "__"
}

func parsePortfolios(userId int) {
	m := UserPortfolios[userId]
	if m != nil {
		return
	}
	m = make(map[string]*Portfolio)
	UserPortfolios[userId] = m
	p := GetPath(userId)
	stat, err := os.Stat(p)
	tmp := false
	if err == nil {
		if !stat.IsDir() {
			os.Remove(p)
			tmp = true
		}
	} else {
		tmp = true
	}
	if tmp {
		err := os.Mkdir(p, 0755)
		if err != nil {
			log.Fatal(err)
		}
		err = copy("template.ini", path.Join(p, "template.ini"))
		if err != nil {
			log.Fatal(err)
		}
		err = ioutil.WriteFile(path.Join(p, "__init__.py"), nil, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}
	files, err := ioutil.ReadDir(p)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		fn := path.Join(p, f.Name())
		if path.Ext(fn) == ".ini" {
			cfg, err := ParseIniFile(fn)
			if err != nil {
				log.Println("failed to load", fn+":", err.Error())
			}
			portfolio, err := ParsePortfolio(cfg, p)
			if err != nil {
				log.Println("error when loading", fn+":", err.Error())
			}
			if portfolio.Name == "" {
				portfolio.Name = f.Name()[0 : len(f.Name())-len(path.Ext(fn))]
			}
			if portfolio.AccPatterns == "" {
				portfolio.AccPatterns = "*"
			}
			m[portfolio.Name] = portfolio
		}
	}
}

func GetFiles(userId int) []string {
	p := GetPath(userId)
	files, err := ioutil.ReadDir(p)
	out := []string{}
	if err != nil {
		return out
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if f.Name() == "__init__.py" {
			continue
		}
		if path.Ext(f.Name()) == ".pyc" {
			continue
		}
		if strings.HasPrefix(f.Name(), ".") {
			continue
		}
		out = append(out, f.Name())
	}
	return out
}

func GetFile(userId int, fn string) (data []byte, err error) {
	fn = path.Join(GetPath(userId), fn)
	data, err = ioutil.ReadFile(fn)
	return
}

func DeleteFile(userId int, fn string) error {
	log.Println("delete file:", fn, userId)
	err := os.Remove(path.Join(GetPath(userId), fn))
	if path.Ext(fn) == ".py" {
		os.Remove(path.Join(GetPath(userId), fn+"c"))
	}
	delete(UserPortfolios, userId)
	parsePortfolios(userId)
	return err
}

func SaveFile(userId int, fn string, content string) error {
	log.Println("save file:", fn, userId)
	err := ioutil.WriteFile(path.Join(GetPath(userId), fn), []byte(content), 0755)
	if path.Ext(fn) == ".py" {
		RestartPy()
	}
	delete(UserPortfolios, userId)
	parsePortfolios(userId)
	return err
}

func getAccMatch(patternsStr string, values []int) []int {
	res := make([]int, 0, len(values))
	if patternsStr != "" {
		if patternsStr[0] == '~' {
			patternsStr = "*," + patternsStr
		}
	}
	for _, p := range split(patternsStr, ",") {
		if p == "" {
			continue
		}
		exclude := p[0] == '~'
		if exclude {
			p = p[1:]
		}
		for _, v := range values {
			name := AccNames[v]
			matched, _ := filepath.Match(p, name)
			if !matched {
				continue
			}
			if exclude {
				i := funk.IndexOf(res, v)
				if i < 0 {
					continue
				}
				res = append(res[:i], res[i+1:]...)
			} else {
				i := funk.IndexOf(res, v)
				if i < 0 {
					res = append(res, v)
				}
			}
		}
	}
	return res
}

func RunUserPortfolios() map[int]map[string]interface{} {
	out := make(map[int]map[string]interface{})
	var wg sync.WaitGroup
	wg.Add(len(UserIdAccs))
	for userId, accs := range UserIdAccs {
		rpt := make(map[string]interface{})
		out[userId] = rpt
		go func() {
			defer wg.Done()
			for _, p := range UserPortfolios[userId] {
				usedAccs := getAccMatch(p.AccPatterns, accs)
				var positions []*Position
				if len(usedAccs) > 0 {
					for _, acc := range usedAccs {
						tmp := Positions[acc]
						for _, tmp2 := range tmp {
							if p.Filter != nil {
								v, _ := Evaluate(p.Filter, tmp2)
								if v2, ok2 := v.(bool); ok2 {
									if !v2 {
										continue
									}
								}
							}
							positions = append(positions, tmp2)
						}
					}
				}
				if len(positions) > 0 {
					rpt[p.Name] = p.Run(positions)
				}
			}
		}()
	}
	wg.Wait()
	return out
}
