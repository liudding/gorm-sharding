# gorm-sharding


### Example
```go
DB, err := gorm.Open(mysql.Open(""), &gorm.Config{})
if err != nil {
    t.Fatalf("failed to connect db, got error: %v", err)
}

db001 := mysql.Open("")
db002 := mysql.Open("")

err = DB.Use(sharding.Register(&sharding.DBResolver{
    Sources: map[string]gorm.Dialector{
        "db_001": db001,
        "db_002": db002,
    },
    ShardingColumn: "id",
    ShardingAlgorithm: func(v interface{}) (dbname string, err error) {
        if vv, ok := v.(string); ok {
            i, _ := strconv.ParseInt(vv[len(vv)-3:], 36, 64)
            dbname = fmt.Sprintf("db_%03d", i%2+1)
            return
        }
		
        return "", nil
    },
}))

DB.Model(Book{}).Where("id = ?", "1234").First(&b)
```