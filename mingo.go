package mingo

import (
	"errors"
	"time"

	logx "github.com/my0sot1s/godef/log"
	def "github.com/my0sot1s/godef/sdef"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ObjectHex wrap ObjectIdHex
var oh = bson.ObjectIdHex

//DbConnector wrap any connector to db
type DbConnector struct {
	mgodb *mgo.Database
}

// InitMongo is initial a new connection
func (c *DbConnector) InitMongo(host, user, pw, dbname string) {
	// session, err := mgo.Dial(url)
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:    []string{host},
		Timeout:  60 * time.Second,
		Database: dbname,
		Username: user,
		Password: pw,
	})
	if err != nil {
		logx.ErrLog(err)
		return
	}
	session.SetMode(mgo.Monotonic, true)
	c.mgodb = session.DB(dbname)
	logx.Log("+ DB CONNECTED DBNAME : ", dbname)
}

func castRaw2Real(m def.M) def.M {
	delete(m, "__v")
	return m
}

// Insert insert Single
func (c *DbConnector) Insert(coll string, data def.M) (def.M, error) {
	data["_id"] = bson.NewObjectId()
	er := c.mgodb.C(coll).Insert(data)
	if er != nil {
		logx.ErrLog(er)
		return nil, er
	}
	data["id"] = data["_id"]
	return data, nil
}

// Update single value
func (c *DbConnector) Update(coll string, selector def.M, updater def.M) (def.M, error) {
	if selector["_id"] == nil && selector["id"] != nil {
		selector["_id"] = selector["id"]
	}
	if selector["_id"] == nil {
		return nil, errors.New("selector nil _id")
	}
	delete(selector, "id")

	delete(updater, "_id")
	delete(updater, "id")
	// set updater
	update := def.M{"$set": updater}
	// update
	er := c.mgodb.C(coll).Update(selector, update)

	if er != nil {
		logx.ErrLog(er)
		return nil, er
	}

	for k, v := range updater {
		selector[k] = v
	}

	return updater, nil
}

// Delete Single value
func (c *DbConnector) Delete(coll string, selector def.M) error {
	if selector["_id"] == nil {
		return errors.New("selector nil _id")
	}
	er := c.mgodb.C(coll).Remove(selector)
	if er != nil {
		logx.ErrLog(er)
		return er
	}
	return nil
}
