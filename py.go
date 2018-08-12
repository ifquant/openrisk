package main

import (
	"fmt"
	"github.com/sbinet/go-python"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
)

var pySymbol = python.PyString_FromString("Symbol")
var pyLocalSymbol = python.PyString_FromString("LocalSymbol")
var pyBbgid = python.PyString_FromString("Bbgid")
var pyCusip = python.PyString_FromString("Cusip")
var pySedol = python.PyString_FromString("Sedol")
var pyIsin = python.PyString_FromString("Isin")
var pySector = python.PyString_FromString("Sector")
var pyIndustry = python.PyString_FromString("Industry")
var pySubIndustry = python.PyString_FromString("SubIndustry")
var pyMarket = python.PyString_FromString("Market")
var pyType = python.PyString_FromString("Type")
var pyCurrency = python.PyString_FromString("Currency")
var pyMultiplier = python.PyString_FromString("Multiplier")
var pyRate = python.PyString_FromString("Rate")
var pyAdv20 = python.PyString_FromString("Adv20")
var pyMarketCap = python.PyString_FromString("MarketCap")
var pyPrevClose = python.PyString_FromString("PrevClose")
var pyOpen = python.PyString_FromString("Open")
var pyHigh = python.PyString_FromString("High")
var pyLow = python.PyString_FromString("Low")
var pyClose = python.PyString_FromString("Close")
var pyQty = python.PyString_FromString("Qty")
var pyVol = python.PyString_FromString("Vol")
var pyVwap = python.PyString_FromString("Vwap")
var pyAsk = python.PyString_FromString("Ask")
var pyBid = python.PyString_FromString("Bid")
var pyAskSize = python.PyString_FromString("AskSize")
var pyBidSize = python.PyString_FromString("BidSize")
var pyOutstandBuyQty = python.PyString_FromString("OutstandBuyQty")
var pyOutstandSellQty = python.PyString_FromString("OutstandSellQty")
var pyAcc = python.PyString_FromString("Acc")
var pyPos = python.PyString_FromString("Pos")
var pyAvgPx = python.PyString_FromString("AvgPx")
var pyRealizedPnl = python.PyString_FromString("RealizedPnl")
var pyPos0 = python.PyString_FromString("Pos0")
var pyBuyQty = python.PyString_FromString("BuyQty")
var pySellQty = python.PyString_FromString("SellQty")
var pyBuyValue = python.PyString_FromString("BuyValue")
var pySellValue = python.PyString_FromString("SellValue")

func (p *Position) ToPy() *python.PyObject {
	out := python.PyDict_New()
	s := p.Security
	python.PyDict_SetItem(out, pySymbol, python.PyString_FromString(s.Symbol))
	python.PyDict_SetItem(out, pyLocalSymbol, python.PyString_FromString(s.LocalSymbol))
	python.PyDict_SetItem(out, pyBbgid, python.PyString_FromString(s.Bbgid))
	python.PyDict_SetItem(out, pyCusip, python.PyString_FromString(s.Cusip))
	python.PyDict_SetItem(out, pySedol, python.PyString_FromString(s.Sedol))
	python.PyDict_SetItem(out, pyIsin, python.PyString_FromString(s.Isin))
	python.PyDict_SetItem(out, pySector, python.PyString_FromString(s.Sector))
	python.PyDict_SetItem(out, pyIndustry, python.PyString_FromString(s.Industry))
	python.PyDict_SetItem(out, pySubIndustry, python.PyString_FromString(s.SubIndustry))
	python.PyDict_SetItem(out, pyMarket, python.PyString_FromString(s.Market))
	python.PyDict_SetItem(out, pyType, python.PyString_FromString(s.Type))
	python.PyDict_SetItem(out, pyCurrency, python.PyString_FromString(s.Currency))
	python.PyDict_SetItem(out, pyMultiplier, python.PyFloat_FromDouble(s.Multiplier))
	python.PyDict_SetItem(out, pyRate, python.PyFloat_FromDouble(s.Rate))
	python.PyDict_SetItem(out, pyAdv20, python.PyFloat_FromDouble(s.Adv20))
	python.PyDict_SetItem(out, pyMarketCap, python.PyFloat_FromDouble(s.MarketCap))
	python.PyDict_SetItem(out, pyPrevClose, python.PyFloat_FromDouble(s.PrevClose))
	python.PyDict_SetItem(out, pyOpen, python.PyFloat_FromDouble(s.Open))
	python.PyDict_SetItem(out, pyHigh, python.PyFloat_FromDouble(s.High))
	python.PyDict_SetItem(out, pyLow, python.PyFloat_FromDouble(s.Low))
	close := s.GetClose()
	python.PyDict_SetItem(out, pyClose, python.PyFloat_FromDouble(close))
	python.PyDict_SetItem(out, pyQty, python.PyFloat_FromDouble(s.Qty))
	python.PyDict_SetItem(out, pyVol, python.PyFloat_FromDouble(s.Vol))
	python.PyDict_SetItem(out, pyVwap, python.PyFloat_FromDouble(s.Vwap))
	python.PyDict_SetItem(out, pyAsk, python.PyFloat_FromDouble(s.Ask))
	python.PyDict_SetItem(out, pyBid, python.PyFloat_FromDouble(s.Bid))
	python.PyDict_SetItem(out, pyAskSize, python.PyFloat_FromDouble(s.AskSize))
	python.PyDict_SetItem(out, pyBidSize, python.PyFloat_FromDouble(s.BidSize))
	python.PyDict_SetItem(out, pyOutstandBuyQty, python.PyFloat_FromDouble(p.OutstandBuyQty))
	python.PyDict_SetItem(out, pyOutstandSellQty, python.PyFloat_FromDouble(p.OutstandSellQty))
	name := AccNames[p.Acc]
	python.PyDict_SetItem(out, pyAcc, python.PyString_FromString(name))
	python.PyDict_SetItem(out, pyPos, python.PyFloat_FromDouble(p.Qty))
	python.PyDict_SetItem(out, pyAvgPx, python.PyFloat_FromDouble(p.AvgPx))
	python.PyDict_SetItem(out, pyRealizedPnl, python.PyFloat_FromDouble(p.RealizedPnl))
	python.PyDict_SetItem(out, pyPos0, python.PyFloat_FromDouble(p.Bod.Qty))
	python.PyDict_SetItem(out, pyBuyQty, python.PyFloat_FromDouble(p.BuyQty))
	python.PyDict_SetItem(out, pySellQty, python.PyFloat_FromDouble(p.SellQty))
	python.PyDict_SetItem(out, pyBuyValue, python.PyFloat_FromDouble(p.BuyValue))
	python.PyDict_SetItem(out, pySellValue, python.PyFloat_FromDouble(p.SellValue))

	return out
}

func InitPy() {
	os.Setenv("PYTHONPATH", "."+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"))
	err := python.Initialize()
	if err != nil {
		log.Panic(err.Error())
	}
}

func RestartPy() {
	python.Finalize()
	InitPy()
}

func CallPy(moduleName string, funcName string, strArgs string, positions []*Position, mpath string) (res interface{}, eres error) {
	if mpath != "" {
		_, err := os.Stat(path.Join(mpath, moduleName+".py"))
		if err == nil {
			moduleName = mpath + "." + moduleName
		}
	}
	module := python.PyImport_ImportModule(moduleName)
	if module == nil {
		eres = fmt.Errorf("could not import python module: " + moduleName)
		return
	}
	myfunc := module.GetAttrString(funcName)
	if myfunc == nil {
		eres = fmt.Errorf("could not get function: " + funcName + ", from python module: " + moduleName)
		return
	}
	l := python.PyList_New(len(positions))
	for i, p := range positions {
		python.PyList_SetItem(l, i, p.ToPy())
	}
	args := python.PyTuple_New(2)
	python.PyTuple_SetItem(args, 0, l)
	python.PyTuple_SetItem(args, 1, python.PyString_FromString(strArgs))
	out := myfunc.Call(args, python.PyDict_New())
	if out == nil {
		return
	}
	if python.PyFloat_Check(out) {
		res = python.PyFloat_AsDouble(out)
		return
	}
	if python.PyInt_Check(out) {
		res = float64(python.PyInt_AsLong(out))
		return
	}
	if python.PyList_Check(out) {
		var res2 []interface{}
		n := python.PyList_Size(out)
		for i := 0; i < n; i++ {
			item := python.PyList_GetItem(out, i)
			if python.PyList_Check(item) {
				n2 := python.PyList_Size(item)
				if n2 == 2 {
					a := python.PyList_GetItem(item, 0)
					var name string
					if python.PyString_Check(a) {
						name = python.PyString_AsString(a)
					}
					var value float64
					b := python.PyList_GetItem(item, 1)
					if python.PyFloat_Check(b) {
						value = python.PyFloat_AsDouble(b)
					}
					if python.PyInt_Check(b) {
						value = float64(python.PyInt_AsLong(b))
					}
					res2 = append(res2, []interface{}{name, value})
				}
			}
			if python.PyTuple_Check(item) {
				n2 := python.PyTuple_Size(item)
				a := python.PyTuple_GetItem(item, 0)
				if n2 == 2 {
					var name string
					if python.PyString_Check(a) {
						name = python.PyString_AsString(a)
					}
					var value float64
					b := python.PyTuple_GetItem(item, 1)
					if python.PyFloat_Check(b) {
						value = python.PyFloat_AsDouble(b)
					}
					if python.PyInt_Check(b) {
						value = float64(python.PyInt_AsLong(b))
					}
					res2 = append(res2, []interface{}{name, value})
				}
			}
		}
		res = res2
		return
	}
	return
}

func CheckPy(fn string) error {
	cmd := exec.Command("python", fn)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	slurp, _ := ioutil.ReadAll(stderr)
	msg := string(slurp)
	if msg == "" {
		return nil
	}
	return fmt.Errorf(msg)
}
