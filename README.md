# OpenRisk
Open source post-trade risk management system

[Demo](http://demo.opentradesolutions.com/#/risk)

# Steps to run
Make sure opentrade is running, openrisk connects to opentrade and share its frontend
```bash
go get github.com/opentradesolutions/openrisk
cd .gopath/src/github.com/opentradesolutions/openrisk
dep ensure
make run
```
Now, you can open "http://localhost:9111/#/risk" on your browser.
