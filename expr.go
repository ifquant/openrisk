package main

import (
	"fmt"
	"github.com/Knetic/govaluate"
	"math"
	"reflect"
	"strconv"
	"strings"
)

type Expression struct {
	E *govaluate.EvaluableExpression
	A string    // aggregate function name
	N int       // for A == "top"
	C [3]string // for call()
}

var predefinedFunctions = map[string]govaluate.ExpressionFunction{
	"min": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		b := args[1].(float64)
		return math.Min(a, b), nil
	},
	"max": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		b := args[1].(float64)
		return math.Max(a, b), nil
	},
	"pow": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		b := args[1].(float64)
		return math.Pow(a, b), nil
	},
	"sqrt": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Sqrt(a), nil
	},
	"round": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Round(a), nil
	},
	"isNaN": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.IsNaN(a), nil
	},
	"ceil": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Ceil(a), nil
	},
	"floor": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Floor(a), nil
	},
	"exp": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Exp(a), nil
	},
	"exp2": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Exp2(a), nil
	},
	"abs": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Abs(a), nil
	},
	"log": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Log(a), nil
	},
	"log2": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Log2(a), nil
	},
	"log10": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.Log10(a), nil
	},
	"isInf": func(args ...interface{}) (interface{}, error) {
		a := args[0].(float64)
		return math.IsInf(a, -1) || math.IsInf(a, 1), nil
	},
	"strlen": func(args ...interface{}) (interface{}, error) {
		length := len(args[0].(string))
		return float64(length), nil
	},
}

func ParseExpr(ln string, expr string, name string, params map[string]interface{}, valueTmpl interface{}, path string) (res *Expression, eres error) {
	var a string
	var n int
	if strings.HasPrefix(expr, "sum(") {
		a = "sum"
		expr = expr[4 : len(expr)-1]
	} else if strings.HasPrefix(expr, "len(") {
		a = "len"
		expr = expr[4 : len(expr)-1]
	} else if strings.HasPrefix(expr, "mean(") {
		a = "mean"
		expr = expr[5 : len(expr)-1]
	} else if strings.HasPrefix(expr, "std(") {
		a = "std"
		expr = expr[4 : len(expr)-1]
	} else if strings.HasPrefix(expr, "top(") {
		fields := split(expr, ",)")
		if len(fields) > 1 {
			i, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil {
				eres = fmt.Errorf("invalid top expression on line " + ln + ": " + expr + ": missing valid second parameter")
				return
			}
			a = "top"
			n = i
		} else {
			eres = fmt.Errorf("invalid top expression on line " + ln + ": " + expr + ": missing second parameter")
			return
		}
	} else if strings.HasPrefix(expr, "call(") {
		var m string
		var f string
		var p string
		var tmp = map[string]govaluate.ExpressionFunction{
			"call": func(args ...interface{}) (res interface{}, eres error) {
				if len(args) < 2 {
					eres = fmt.Errorf("module name and function name required")
					return
				}
				m = args[0].(string)
				f = args[1].(string)
				if len(args) > 2 {
					p = args[2].(string)
				}
				if m == "" || f == "" {
					eres = fmt.Errorf("module name and function name required")
					return
				}
				res, eres = CallPy(m, f, p, nil, path)
				if res == nil {
					if eres == nil {
						eres = fmt.Errorf(" it must return a float number or an name/value tuple list")
					}
					return
				}
				return
			},
		}
		e, err := govaluate.NewEvaluableExpressionWithFunctions(expr, tmp)
		if err != nil {
			eres = fmt.Errorf("invalid " + name + " expression on line " + ln + ": " + expr + ": " + err.Error())
			return
		}
		_, err = e.Evaluate(nil)
		if err != nil {
			eres = fmt.Errorf("invalid " + name + " expression on line " + ln + ": " + expr + ": " + err.Error())
			return
		}
		res = &Expression{
			C: [3]string{m, f, p},
			A: "call",
		}
		return
	}
	e, err := govaluate.NewEvaluableExpressionWithFunctions(expr, predefinedFunctions)
	if err != nil {
		eres = fmt.Errorf("invalid " + name + " expression on line " + ln + ": " + expr + ": " + err.Error())
		return
	}
	p := &Position{}
	p.Security = &Security{}
	v, err2 := Evaluate(&Expression{E: e}, p, params)
	if err2 != nil {
		eres = fmt.Errorf("invalid " + name + " expression on line " + ln + ": " + expr + ": " + err2.Error())
		return
	}
	if valueTmpl != nil && reflect.TypeOf(v) != reflect.TypeOf(valueTmpl) {
		eres = fmt.Errorf("invalid " + name + " expression on line " + ln + ": " + expr + ": which must return " + reflect.TypeOf(valueTmpl).String())
		return
	}
	res = &Expression{
		E: e,
		A: a,
		N: n,
	}
	return
}

func Evaluate(e *Expression, p *Position, optional ...map[string]interface{}) (interface{}, error) {
	params := make(map[string]interface{}, 60)
	if len(optional) > 0 && optional[0] != nil {
		params = optional[0]
	}
	s := p.Security
	params["Symbol"] = s.Symbol
	params["Sector"] = s.Sector
	params["Industry"] = s.Industry
	params["IndustryGroup"] = s.IndustryGroup
	params["SubIndustry"] = s.SubIndustry
	params["Market"] = s.Market
	params["Type"] = s.Type
	params["Currency"] = s.Currency
	params["Multiplier"] = s.Multiplier
	params["Rate"] = s.Rate
	params["Adv20"] = s.Adv20
	params["MarketCap"] = s.MarketCap
	params["PrevClose"] = s.PrevClose
	params["Open"] = s.Open
	params["High"] = s.High
	params["Low"] = s.Low
	close := s.GetClose()
	params["Close"] = close
	params["Qty"] = s.Qty
	params["Vol"] = s.Vol
	params["Vwap"] = s.Vwap
	params["Ask"] = s.Ask
	params["Bid"] = s.Bid
	params["AskSize"] = s.AskSize
	params["BidSize"] = s.BidSize
	params["OutstandBuyQty"] = p.OutstandBuyQty
	params["OutstandSellQty"] = p.OutstandSellQty
	params["Acc"] = p.Acc
	params["Pos"] = p.Qty
	params["AvgPx"] = p.AvgPx
	params["RealizedPnl"] = p.RealizedPnl
	params["BuyQty"] = p.BuyQty
	params["SellQty"] = p.SellQty
	params["BuyValue"] = p.BuyValue
	params["SellValue"] = p.SellValue
	params["Pos0"] = p.Bod.Qty
	params["NaN"] = math.NaN()
	return e.E.Evaluate(params)
}
