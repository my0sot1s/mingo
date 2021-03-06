package mingo

import (
	"math"

	"gopkg.in/mgo.v2/bson"

	"github.com/my0sot1s/godef/convt"
	logx "github.com/my0sot1s/godef/log"
	def "github.com/my0sot1s/godef/sdef"
)

// readBy is a func for read Db
func (c *DbConnector) readBy(coll, anchor, sortBy string, limit int, conditions def.M) ([]def.M, error) {
	result, query := make([]def.M, 0), def.M{}
	if anchor != "" {
		if limit < 0 {
			//  lt
			query = def.M{"_id": def.M{"$lt": oh(anchor)}}
		} else {
			//  gt
			query = def.M{"_id": def.M{"$gt": oh(anchor)}}
		}
	}
	if conditions != nil {
		for k, c := range conditions {
			query[k] = c
		}
	}
	q := c.mgodb.C(coll).Find(query)
	limit = int(math.Abs(float64(limit)))
	if err := q.Limit(limit).Sort(sortBy).All(&result); err != nil {
		return nil, err
	}
	if len(result) > 0 {
		for _, r := range result {
			r = castRaw2Real(r)
		}
	}
	return result, nil
}

// Read read all by condition
func (c *DbConnector) Read(coll, anchor, sortBy string, limit int, conditions def.M) ([]def.M, error) {
	data, err := c.readBy(coll, anchor, sortBy, limit, conditions)
	if err == nil {
		for _, v := range data {
			v["id"] = v["_id"]
		}
	}
	return data, err
}

// ReadByID get one
func (c *DbConnector) ReadByID(coll, id string) (def.M, error) {
	q := c.mgodb.C(coll)
	if !bson.IsObjectIdHex(id) {
		return nil, convt.CreateError("Is not object hex")
	}
	var result def.M
	e := q.FindId(bson.ObjectIdHex(id)).One(&result)
	if e != nil {
		return nil, e
	}
	result["id"] = result["_id"]
	return result, nil
}

//ReadByIDs get list items by ids
func (c *DbConnector) ReadByIDs(coll string, ids []string) ([]def.M, error) {
	// make condition
	slideIds, cond, result := make([]bson.ObjectId, 0), make(def.M), make([]def.M, 0)
	for _, id := range ids {
		if !bson.IsObjectIdHex(id) {
			return nil, convt.CreateError("Some Is not object hex")
		}
		slideIds = append(slideIds, bson.ObjectIdHex(id))
	}
	cond["_id"] = def.M{"$in": slideIds}
	logx.Log(cond)
	q := c.mgodb.C(coll).Find(cond)
	if err := q.All(&result); err != nil {
		return nil, err
	}
	if len(result) > 0 {
		for _, r := range result {
			r = castRaw2Real(r)
		}
	}
	return result, nil
}
