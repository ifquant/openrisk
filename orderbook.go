package main

import (
	"log"
	"strings"
)

type MD struct {
	Open    float64
	High    float64
	Low     float64
	Close   float64
	Qty     float64
	Vol     float64
	Vwap    float64
	Ask     float64
	Bid     float64
	AskSize float64
	BidSize float64
}

type Security struct {
	Id            int64
	Symbol        string
	LocalSymbol   string
	Bbgid         string
	Cusip         string
	Sedol         string
	Isin          string
	Market        string
	Type          string
	Multiplier    float64
	PrevClose     float64
	Rate          float64
	Currency      string
	Adv20         float64
	MarketCap     float64
	Sector        string
	IndustryGroup string
	Industry      string
	SubIndustry   string
	MD
}

func (s *Security) GetClose() float64 {
	close := s.Close
	if close <= 0 {
		close = s.PrevClose
	}
	return close
}

var SecurityMapById = make(map[int64]*Security)
var SecurityMapByMarket = make(map[string]map[string]*Security)

func ParseSecurity(msg []interface{}) {
	sec := &Security{
		Id:            int64(msg[1].(float64)),
		Symbol:        msg[2].(string),
		Market:        msg[3].(string),
		Type:          msg[4].(string),
		Multiplier:    msg[5].(float64),
		PrevClose:     msg[6].(float64),
		Rate:          msg[7].(float64),
		Currency:      msg[8].(string),
		Adv20:         msg[9].(float64),
		MarketCap:     msg[10].(float64),
		Sector:        msg[11].(string),
		IndustryGroup: msg[12].(string),
		Industry:      msg[13].(string),
		SubIndustry:   msg[14].(string),
		LocalSymbol:   msg[15].(string),
		Bbgid:         msg[16].(string),
		Cusip:         msg[17].(string),
		Sedol:         msg[18].(string),
		Isin:          msg[19].(string),
	}
	if sec.Market == "CURRENCY" {
		sec.Market = "FX"
	}
	if sec.Multiplier <= 0 {
		sec.Multiplier = 1
	}
	if sec.Rate <= 0 {
		sec.Rate = 1
	}
	SecurityMapById[sec.Id] = sec
	tmp := SecurityMapByMarket[sec.Market]
	if tmp == nil {
		tmp = make(map[string]*Security)
		SecurityMapByMarket[sec.Market] = tmp
	}
	tmp[sec.Symbol] = sec
}

type Order struct {
	Id          int64
	OrigClOrdId int64
	// Tm int64
	// Seq int64
	St       string
	Security *Security
	// UserId int
	Acc  int
	Qty  float64
	Px   float64
	Side string
	Type string
	// Tif string
	CumQty  float64
	AvgPx   float64
	LastQty float64
	LastPx  float64
}

var orders = make(map[int64]*Order)

type PositionBase struct {
	Qty         float64
	AvgPx       float64
	RealizedPnl float64
}

type Position struct {
	PositionBase
	Bod             PositionBase
	OutstandBuyQty  float64
	OutstandSellQty float64
	BuyQty          float64
	BuyValue        float64
	SellQty         float64
	SellValue       float64
	Security        *Security
	Acc             int
}

var Positions = make(map[int]map[int64]*Position)
var usedSecurities = make(map[int64]bool)

func getPos(acc int, securityId int64) *Position {
	tmp := Positions[acc]
	if tmp == nil {
		tmp = make(map[int64]*Position)
		Positions[acc] = tmp
	}
	p := tmp[securityId]
	if p == nil {
		p = &Position{}
		p.Acc = acc
		p.Security = SecurityMapById[securityId]
		if p.Security == nil {
			log.Println("unknown securityId", securityId)
			return p
		}
		tmp[securityId] = p
		used := usedSecurities[securityId]
		if !used {
			Request([]interface{}{"sub", securityId})
			usedSecurities[securityId] = true
		}
	}
	return p
}

func isLive(st string) bool {
	if st == "" {
		return true
	}
	st = strings.ToLower(st)
	return strings.HasPrefix(st, "pending") || strings.HasPrefix(st, "unconfirmed") || strings.HasPrefix(st, "partial") || st == "new"
}

func updatePos(ord *Order) {
	securityId := ord.Security.Id
	p := getPos(ord.Acc, securityId)
	var outstand *float64
	if ord.Side == "buy" {
		outstand = &p.OutstandBuyQty
	} else {
		outstand = &p.OutstandSellQty
	}

	switch ord.St {
	case "unconfirmed", "unconfirmed_replace":
		*outstand += ord.Qty - ord.CumQty
	case "filled", "partial":
		if ord.LastQty > 0 && ord.Type != "otc" {
			*outstand -= ord.LastQty
			if *outstand < 0 {
				log.Printf("Outstand < 0: %s", ord)
				*outstand = 0
			}
		}
		qty := ord.LastQty
		if ord.Side == "buy" {
			p.BuyQty += ord.LastQty
			p.BuyValue += ord.LastQty * ord.LastPx
		} else {
			qty = -qty
			p.SellQty += ord.LastQty
			p.SellValue += ord.LastQty * ord.LastPx
		}
		qty0 := p.Qty
		px := ord.LastPx
		multiplier := ord.Security.Rate * ord.Security.Multiplier
		if (qty0 > 0) && (qty < 0) { // sell trade to cover position
			if qty0 > -qty {
				p.RealizedPnl += (px - p.AvgPx) * -qty * multiplier
			} else {
				p.RealizedPnl += (px - p.AvgPx) * qty0 * multiplier
				p.AvgPx = px
			}
		} else if (qty0 < 0) && (qty > 0) { // buy trade to cover position
			if -qty0 > qty {
				p.RealizedPnl += (p.AvgPx - px) * qty * multiplier
			} else {
				p.RealizedPnl += (p.AvgPx - px) * -qty0 * multiplier
				p.AvgPx = px
			}
		} else { // open position
			p.AvgPx = (qty0*p.AvgPx + qty*px) / (qty0 + qty)
		}
		p.Qty += qty

	default:
		*outstand -= ord.Qty - ord.CumQty
		if *outstand < 0 {
			log.Printf("Outstand < 0: %s", ord)
			*outstand = 0
		}
	}
}

var seqNum int64 = 0
var offlineDone = false
var onlineCache [][]interface{}

func ParseOffline(msg []interface{}) {
	if msg[1].(string) == "complete" {
		for _, msg := range onlineCache {
			ParseOrder(msg, false)
		}
		onlineCache = onlineCache[:0]
		offlineDone = true
		log.Print("offline done")
	}
}

func ParseOrder(msg []interface{}, isOnline bool) {
	if isOnline && !offlineDone {
		onlineCache = append(onlineCache, msg)
		return
	}
	clOrdId := int64(msg[1].(float64))
	// tm := int64(msg[2].(float64))
	seq := int64(msg[3].(float64))
	if seq <= seqNum {
		return
	}
	seqNum = seq
	switch st := msg[4].(string); st {
	case "unconfirmed", "unconfirmed_replace":
		securityId := int64(msg[5].(float64))
		security := SecurityMapById[securityId]
		if security == nil {
			log.Println("not found security", securityId)
			return
		}
		// aid := int(msg[6].(float64))
		// userId := int(msg[7].(float64))
		acc := int(msg[8].(float64))
		// brokerAcc := int(msg[9].(float65))
		qty := msg[10].(float64)
		px := msg[11].(float64)
		side := msg[12].(string)
		// ordType := msg[13].(string)
		// tif := msg[14].(string)
		ord := Order{
			Id:       clOrdId,
			St:       st,
			Security: security,
			Acc:      acc,
			Qty:      qty,
			Px:       px,
			Side:     side,
		}
		if st == "unconfirmed_replace" {
			origClOrdId := int64(msg[14].(float64))
			ord.OrigClOrdId = origClOrdId
		}
		orders[clOrdId] = &ord
		updatePos(&ord)
	case "filled", "partial":
		qty := msg[5].(float64)
		px := msg[6].(float64)
		// tradeId = msg[7].(string)
		execTransType := msg[8].(string)
		if execTransType == "cancel" {
			qty = -qty
		}
		ord := orders[clOrdId]
		if ord != nil {
			ord.AvgPx = (ord.CumQty*ord.AvgPx + qty*px) / (ord.CumQty + qty)
			ord.CumQty += qty
			if ord.CumQty > ord.Qty {
				log.Printf("overfill found: %s", msg)
			}
			ord.LastQty = qty
			ord.LastPx = px
			if ord.CumQty >= ord.Qty {
				st = "filled"
			} else {
				st = "partial"
			}
			ord.St = st
			updatePos(ord)
		} else {
			log.Println("not found order for", clOrdId)
		}
	case "cancelled":
		ord := orders[clOrdId]
		if ord != nil {
			st0 := ord.St
			ord.St = st
			if isLive(st0) {
				updatePos(ord)
			}
		} else {
			log.Println("can not find order for", clOrdId)
		}
	case "new", "replaced":
		ord := orders[clOrdId]
		if ord != nil {
			if st == "replaced" {
				old := orders[ord.OrigClOrdId]
				if old == nil {
					log.Println("can not find order for", ord.OrigClOrdId)
				} else {
					old.St = st
				}
				st = "confirmed"
			}
			ord.St = st
		} else {
			log.Println("can not find order for", clOrdId)
		}
	case "new_rejected", "replace_rejected":
		ord := orders[clOrdId]
		if ord != nil {
			st0 := ord.St
			ord.St = st
			if isLive(st0) {
				updatePos(ord)
			}
		} else {
			log.Println("can not find order for", clOrdId)
		}
	case "risk_rejected":
		ord := orders[clOrdId]
		if ord != nil {
			ord.St = st
			updatePos(ord)
		}
	}
}

func ParseBod(msg []interface{}) {
	acc := int(msg[1].(float64))
	securityId := int64(msg[2].(float64))
	qty := msg[3].(float64)
	avgPx := msg[4].(float64)
	realizedPnl := msg[5].(float64)
	// brokerAcc := int(msg[4])
	// tm := int64(msg[5].(float64))
	p := getPos(acc, securityId)
	p.Qty = qty
	p.AvgPx = avgPx
	p.RealizedPnl = realizedPnl
	p.Bod.Qty = qty
	p.Bod.AvgPx = avgPx
	p.Bod.RealizedPnl = realizedPnl
}

func ParseMd(msg []interface{}) {
	for i := 1; i < len(msg); i++ {
		data := msg[i].([]interface{})
		securityId := int64(data[0].(float64))
		md := data[1].(map[string]interface{})
		for k, _v := range md {
			v := _v.(float64)
			s := SecurityMapById[securityId]
			if s == nil {
				log.Println("unknown security id", securityId)
				continue
			}
			switch k {
			case "o":
				s.Open = v
			case "h":
				s.High = v
			case "l":
				s.Low = v
			case "c":
				s.Close = v
			case "q":
				s.Qty = v
			case "v":
				s.Vol = v
			case "V":
				s.Vwap = v
			case "a0":
				s.Ask = v
			case "b0":
				s.Bid = v
			case "A0":
				s.AskSize = v
			case "B0":
				s.BidSize = v
			}
		}
	}
}
