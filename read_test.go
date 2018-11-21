package mingo

import (
	"testing"

	logx "github.com/my0sot1s/godef/log"
)

var host, user, pw, dbname = "", "", "", ""

func Test_ReadIDs(t *testing.T) {
	c := &DbConnector{}
	c.InitMongo(host, user, pw, dbname)
	list := []string{
		"5bf02c2ae7f3735a9e1757fb",
		"5bf02c2ae7f3735a9e1757fc",
	}
	items, err := c.ReadByIDs("Product", list)
	logx.Log(items, err)
}
