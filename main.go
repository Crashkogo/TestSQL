package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"log"
	"os"
	"sync"
	"time"
)

// var err error
var jsonMsg []byte
var sqlmsg ordWB
var tString string = "{\n  \"order_uid\": \"b563feb7b2b84b6test\",\n  \"track_number\": \"WBILMTESTTRACK\",\n  \"entry\": \"WBIL\",\n  \"delivery\": {\n    \"name\": \"Test Testov\",\n    \"phone\": \"+9720000000\",\n    \"zip\": \"2639809\",\n    \"city\": \"Kiryat Mozkin\",\n    \"address\": \"Ploshad Mira 15\",\n    \"region\": \"Kraiot\",\n    \"email\": \"test@gmail.com\"\n  },\n  \"payment\": {\n    \"transaction\": \"b563feb7b2b84b6test\",\n    \"request_id\": \"\",\n    \"currency\": \"USD\",\n    \"provider\": \"wbpay\",\n    \"amount\": 1817,\n    \"payment_dt\": 1637907727,\n    \"bank\": \"alpha\",\n    \"delivery_cost\": 1500,\n    \"goods_total\": 317,\n    \"custom_fee\": 0\n  },\n  \"items\": [\n    {\n      \"chrt_id\": 9934930,\n      \"track_number\": \"WBILMTESTTRACK\",\n      \"price\": 453,\n      \"rid\": \"ab4219087a764ae0btest\",\n      \"name\": \"Mascaras\",\n      \"sale\": 30,\n      \"size\": \"0\",\n      \"total_price\": 317,\n      \"nm_id\": 2389212,\n      \"brand\": \"Vivienne Sabo\",\n      \"status\": 202\n    }\n  ],\n  \"locale\": \"en\",\n  \"internal_signature\": \"\",\n  \"customer_id\": \"test\",\n  \"delivery_service\": \"meest\",\n  \"shardkey\": \"9\",\n  \"sm_id\": 99,\n  \"date_created\": \"2021-11-26T06:22:19Z\",\n  \"oof_shard\": \"1\"\n}"
var retID int8

//var allCache Cache
//var items map[int8]ordWB

func main() {

	db, err := sql.Open("pgx", "postgres://postgres:Parol123!@localhost:5432/wb_l0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	jsonMsg = []byte(tString)

	err3 := json.Unmarshal(jsonMsg, &sqlmsg)
	if err3 != nil {
		log.Println(err3)
	}
	//инициализируем кэш
	myCache := New(0*time.Minute, 0*time.Minute)

	err = db.QueryRow("INSERT INTO orders(id,order_uid,track_number,entry,locale,internal_signature,customer_id,delivery_service,shardkey,sm_id,date_created,oof_shard) VALUES (default, $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id", sqlmsg.OrderUID, sqlmsg.TrackNumber, sqlmsg.Entry, sqlmsg.Locale, sqlmsg.InternalSignature, sqlmsg.CustomerID, sqlmsg.DeliveryService, sqlmsg.Shardkey, sqlmsg.SmID, sqlmsg.DateCreated, sqlmsg.OofShard).Scan(&retID)
	if err != nil {
		log.Println(err)
	}
	//fmt.Println(retID)
	_, err = db.Exec("INSERT INTO delivery(id,name,phone,zip,city,adress,region,email,orderid) VALUES (default,$1, $2, $3, $4, $5, $6, $7, $8)", sqlmsg.Delivery.Name, sqlmsg.Delivery.Phone, sqlmsg.Delivery.Zip, sqlmsg.Delivery.City, sqlmsg.Delivery.Address, sqlmsg.Delivery.Region, sqlmsg.Delivery.Email, retID)
	if err != nil {
		log.Println(err)
	}
	//fmt.Println(sqlmsg.Delivery)
	_, err = db.Exec("INSERT INTO payment(id,transaction,request_id,currency,provider,amount,payment_dt,bank,delivery_cost,goods_total,custom_fee,orderid) VALUES (default,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", sqlmsg.Payment.Transaction, sqlmsg.Payment.RequestID, sqlmsg.Payment.Currency, sqlmsg.Payment.Provider, sqlmsg.Payment.Amount, sqlmsg.Payment.PaymentDt, sqlmsg.Payment.Bank, sqlmsg.Payment.DeliveryCost, sqlmsg.Payment.GoodsTotal, sqlmsg.Payment.CustomFee, retID)
	if err != nil {
		log.Println(err)
	}
	//fmt.Println(sqlmsg.Payment)
	_, err = db.Exec("INSERT INTO items(id,chrt_id,track_number,price,rid,name,sale,size,total_price,nm_id,brand,status,orderid) VALUES (default,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", sqlmsg.Items[0].ChrtID, sqlmsg.Items[0].TrackNumber, sqlmsg.Items[0].Price, sqlmsg.Items[0].Rid, sqlmsg.Items[0].Name, sqlmsg.Items[0].Sale, sqlmsg.Items[0].Size, sqlmsg.Items[0].TotalPrice, sqlmsg.Items[0].NmID, sqlmsg.Items[0].Brand, sqlmsg.Items[0].Status, retID)
	if err != nil {
		fmt.Println(err)
	}
	myCache.Set(retID, sqlmsg, 0*time.Minute)

	defer db.Close()

	lineCache, validKey := myCache.Get(retID)
	if validKey == false {
		fmt.Println("Нет такой записи")
	}
	fmt.Println(lineCache)

	var greeting string
	err = db.QueryRow("select 'Hello, world!'").Scan(&greeting)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(greeting)
}
func New(defaultExpiration, cleanupInterval time.Duration) *Cache {

	// инициализируем карту(map) в паре ключ(string)/значение(Item)
	items := make(map[int8]Item)

	cache := Cache{
		items:             items,
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
	}

	// Если интервал очистки больше 0, запускаем GC (удаление устаревших элементов)
	if cleanupInterval > 0 {
		cache.StartGC() // данный метод рассматривается ниже
	}

	return &cache
}
func (c *Cache) Set(key int8, value interface{}, duration time.Duration) {

	var expiration int64

	// Если продолжительность жизни равна 0 - используется значение по-умолчанию
	if duration == 0 {
		duration = c.defaultExpiration
	}

	// Устанавливаем время истечения кеша
	if duration > 0 {
		expiration = time.Now().Add(duration).UnixNano()
	}

	c.Lock()

	defer c.Unlock()

	c.items[key] = Item{
		Value:      value,
		Expiration: expiration,
		Created:    time.Now(),
	}

}
func (c *Cache) Get(key int8) (interface{}, bool) {

	c.RLock()

	defer c.RUnlock()

	item, found := c.items[key]

	// ключ не найден
	if !found {
		return nil, false
	}

	// Проверка на установку времени истечения, в противном случае он бессрочный
	if item.Expiration > 0 {

		// Если в момент запроса кеш устарел возвращаем nil
		if time.Now().UnixNano() > item.Expiration {
			return nil, false
		}

	}

	return item.Value, true
}
func (c *Cache) Delete(key int8) error {

	c.Lock()

	defer c.Unlock()

	if _, found := c.items[key]; !found {
		return errors.New("Key not found")
	}

	delete(c.items, key)

	return nil
}

type Cache struct {
	sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	items             map[int8]Item
}
type Item struct {
	Value      interface{}
	Created    time.Time
	Expiration int64
}
type ordWB struct {
	OrderUID    string `json:"order_uid"`
	TrackNumber string `json:"track_number"`
	Entry       string `json:"entry"`
	Delivery    struct {
		Name    string `json:"name"`
		Phone   string `json:"phone"`
		Zip     string `json:"zip"`
		City    string `json:"city"`
		Address string `json:"address"`
		Region  string `json:"region"`
		Email   string `json:"email"`
	} `json:"delivery"`
	Payment struct {
		Transaction  string `json:"transaction"`
		RequestID    string `json:"request_id"`
		Currency     string `json:"currency"`
		Provider     string `json:"provider"`
		Amount       int    `json:"amount"`
		PaymentDt    int    `json:"payment_dt"`
		Bank         string `json:"bank"`
		DeliveryCost int    `json:"delivery_cost"`
		GoodsTotal   int    `json:"goods_total"`
		CustomFee    int    `json:"custom_fee"`
	} `json:"payment"`
	Items []struct {
		ChrtID      int    `json:"chrt_id"`
		TrackNumber string `json:"track_number"`
		Price       int    `json:"price"`
		Rid         string `json:"rid"`
		Name        string `json:"name"`
		Sale        int    `json:"sale"`
		Size        string `json:"size"`
		TotalPrice  int    `json:"total_price"`
		NmID        int    `json:"nm_id"`
		Brand       string `json:"brand"`
		Status      int    `json:"status"`
	} `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerID        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmID              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

func (c *Cache) StartGC() {
	go c.GC()
}

func (c *Cache) GC() {

	for {
		// ожидаем время установленное в cleanupInterval
		<-time.After(c.cleanupInterval)

		if c.items == nil {
			return
		}

		// Ищем элементы с истекшим временем жизни и удаляем из хранилища
		if keys := c.expiredKeys(); len(keys) != 0 {
			c.clearItems(keys)

		}

	}

}

// expiredKeys возвращает список "просроченных" ключей
func (c *Cache) expiredKeys() (keys []int8) {

	c.RLock()

	defer c.RUnlock()

	for k, i := range c.items {
		if time.Now().UnixNano() > i.Expiration && i.Expiration > 0 {
			keys = append(keys, k)
		}
	}

	return
}

// clearItems удаляет ключи из переданного списка, в нашем случае "просроченные"
func (c *Cache) clearItems(keys []int8) {

	c.Lock()

	defer c.Unlock()

	for _, k := range keys {
		delete(c.items, k)
	}
}
