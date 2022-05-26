package main

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/olivere/elastic/v7"
)

// Elasticsearch demo

type Person struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Married bool   `json:"married"`
}

type Employee struct {
	FirstName string   `json:"firstname"`
	LastName  string   `json:"lastname"`
	Age       int      `json:"age"`
	About     string   `json:"about"`
	Interests []string `json:"interests"`
}

type Kubernetes struct {
	Pod_name        string `json:"pod_name"`
	Namespace_name  string `json:"namespace_name"`
	Container_name  string `json:"container_name"`
	Docker_id       string `json:"docker_id"`
	Container_image string `json:"container_image"`
}

type Kt struct {
	Log        string     `json:"log"`
	Time       string     `json:"time"`
	Kubernetes Kubernetes `json:"kubernetes"`
}

func NewESClient() *elastic.Client {
	//client, err := elastic.NewClient(elastic.SetURL("http://47.118.92.136:9920"), elastic.SetSniff(false))
	client, err := elastic.NewClient(elastic.SetURL("http://47.118.92.136:9902"), elastic.SetSniff(false))

	if err != nil {
		panic(err)
	}
	return client
}

//创建索引
func create() {
	client := NewESClient()
	//1.使用结构体方式存入到es里面
	//e1 := Employee{"jane", "Smith", 20, "I like music", []string{"music"}}
	e := Kubernetes{"city-platform-processing", "kt", "city-platform-processing", "laskhflk112233", "kt-harbor-registry.cn-hangzhou.cr.aliyuncs.com/keytop-test/city-platform-processing:feature_v.1.0.7-22"}
	e1 := Kt{"this is a success log", "", e}
	put, err := client.Index().Index("gotest1").Type("kt").Id("2").BodyJson(e1).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("indexed %s to index %s, type %s \n", put.Id, put.Index, put.Type)
}

//字符串方式创建索引
func create1() {
	//使用字符串
	client := NewESClient()
	e1 := `{"firstname":"john","lastname":"smith","age":22,"about":"i like book","interests":["book","music"]}`
	put, err := client.Index().Index("hss").Type("employee").Id("2").BodyJson(e1).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("indexed %s to index %s, type %s \n", put.Id, put.Index, put.Type)
}

//修改
func update() {
	client := NewESClient()
	res, err := client.Update().Index("hss").Type("employee").Id("1").Doc(map[string]interface{}{"age": 88}).Do(context.Background())
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("update age %s \n", res.Result)
}

//删除
func delete() {
	client := NewESClient()
	res, err := client.Delete().Index("hss").Type("employee").Id("1").Do(context.Background())
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("delete result %s", res.Result)
}

func query() {
	var res *elastic.SearchResult
	var err error
	client := NewESClient()
	res, err = client.Search("hss").Type("employee").Do(context.Background())
	printEmployee(res, err)

}

//条件查找
func query1() {
	var res *elastic.SearchResult
	var err error
	client := NewESClient()
	//查找方式一：
	q := elastic.NewQueryStringQuery("kubernetes.container_name:city-platform-processing") //字段查找
	res, err = client.Search("ks-cityendback-test-log-2022.05.07").Type("_doc").Query(q).Do(context.Background())
	printEmployee(res, err)
	// //查找方法二：
	// if res.Hits.TotalHits > 0 {
	// 	fmt.Printf("found a total fo %d Employee", res.Hits.TotalHits)

	// 	for _, hit := range res.Hits.Hits {
	// 		var t Employee
	// 		err := json.Unmarshal(*hit.Source, &t) //另一种取出的方法
	// 		if err != nil {
	// 			fmt.Println("failed")
	// 		}
	// 		fmt.Printf("employee name %s:%s\n", t.FirstName, t.LastName)
	// 	}
	// } else {
	// 	fmt.Printf("found no employee \n")
	// }
}

//年龄大于21的
func query2() {
	var res *elastic.SearchResult
	var err error
	client := NewESClient()
	boolq := elastic.NewBoolQuery()
	boolq.Must(elastic.NewMatchQuery("lastname", "smith"))
	boolq.Filter(elastic.NewRangeQuery("age").Gt(19))
	res, err = client.Search("hss").Type("employee").Query(boolq).Do(context.Background())
	printEmployee(res, err)
}

//模糊查询
func query3() {
	var res *elastic.SearchResult
	var err error
	client := NewESClient()
	//matchPhrase := elastic.NewMatchPhraseQuery("log", "| Error |")
	//matchPhrase := elastic.NewRegexpQuery("log", "\\sError")
	// matchPhrase := elastic.NewBoolQuery()
	// matchPhrase.Filter(elastic.NewRangeQuery("time").Gte("2022-05-05T01:40:28.836240461Z"), elastic.NewRangeQuery("time").Lte("2022-05-05T02:40:28.836240461Z"))
	keyword := " ERROR "
	//keys := fmt.Sprintf("name:*%s*", keyword)
	matchPhrase := elastic.NewBoolQuery()
	matchPhrase.Filter(elastic.NewQueryStringQuery(keyword))
	t := time.Now() //获取当前时间
	ut := t.UTC()   //转换为UTC时间
	aa := time.Now().Local().Format("2006.01.02")
	//vv := fmt.Sprintf("%#v\n", aa)
	//vc := "ks-cityendback-test-log-" + vv
	//h, _ := time.ParseDuration("-1h")
	// h1 := ut.Add(1 * h)
	// aa := h1.Format("2006-01-02T15:04:05.836240461Z") //格式化输出
	//bb := ut.Format("2006-01-02T15:04:05.836240461Z") //格式化输出
	//matchPhrase.Filter(elastic.NewRangeQuery("time").Gte("2022-05-09T01:40:28.836240461Z"), elastic.NewRangeQuery("time").Lte("2022-05-09T05:40:28.836240461Z"))
	matchPhrase.Filter(elastic.NewRangeQuery("time").Gte(ut.Add(-1*time.Hour)), elastic.NewRangeQuery("time").Lte(ut))
	//matchPhrase.Must(elastic.NewMatchQuery("kubernetes.namespace_name", "kt"))
	matchPhrase.MustNot(elastic.NewMatchQuery("kubernetes.namespace_name", "middleware"))
	matchPhrase.MustNot(elastic.NewMatchQuery("kubernetes.namespace_name", "kube-system"))
	matchPhrase.MustNot(elastic.NewMatchQuery("kubernetes.namespace_name", "kubesphere-logging-system"))
	matchPhrase.Must(elastic.NewMatchPhraseQuery("log", keyword))
	matchPhrase.MustNot(elastic.NewMatchPhraseQuery("log", "The error may"))
	matchPhrase.MustNot(elastic.NewMatchPhraseQuery("log", "The error occurred"))
	matchPhrase.MustNot(elastic.NewMatchPhraseQuery("log", "XSS过滤异常"))
	matchPhrase.MustNot(elastic.NewMatchPhraseQuery("log", "500:parkId异常"))
	matchPhrase.MustNot(elastic.NewMatchPhraseQuery("log", "异常信息{}"))
	matchPhrase.MustNot(elastic.NewMatchPhraseQuery("log", "Unexpected exception occurred invoking"))

	//matchPhrase.MustNot()
	res, err = client.Search("ks-cityendback-test-log-" + aa).Type("_doc").Query(matchPhrase).Size(50).Do(context.Background())

	printEmployee(res, err)
}

//打印查询的employee
func printEmployee(res *elastic.SearchResult, err error) {
	if err != nil {
		fmt.Print(err.Error())
		return
	}

	//var typ Employee
	var typ Kt
	for _, item := range res.Each(reflect.TypeOf(typ)) {
		t := item.(Kt)
		//url := "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=bc5cf342-e1c8-4363-8ff3-d5057a68e3fc"
		fmt.Printf("%#v,%#v,%#v\n", t.Log, t.Kubernetes.Container_name, t.Time)
		// msgStr := fmt.Sprintf(`
		// 	{
		//        "msgtype": "markdown",
		//        "markdown": {
		//            "content": "服务报错日志：%+v\n服务：%s"
		//        }
		//   }`, t.Log, t.Kubernetes.Container_name)

		// fmt.Println("发送的消息是：", msgStr)
		// jsonStr := []byte(msgStr)
		// // 发送http请求
		// req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
		// req.Header.Set("Content-Type", "application/json")
		// client := &http.Client{}
		// resp, err := client.Do(req)
		// if err != nil {
		// 	fmt.Println("Error sending to WeChat Work API")
		// 	return
		// }
		// defer resp.Body.Close()
		// body, _ := ioutil.ReadAll(resp.Body)
		// fmt.Println("发送状态:", string(body))

	}

}

func main() {
	// client := NewESClient()

	// fmt.Println("connect to es success")
	// p1 := Person{Name: "huangshuai", Age: 18, Married: false}
	// put1, err := client.Index().
	// 	Index("hsss").
	// 	BodyJson(p1).
	// 	Do(context.Background())
	// if err != nil {
	// 	// Handle error
	// 	panic(err)
	// }
	// fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

	//create() //创建索引
	//update()	//修改
	//delete()  //删除
	//query3()
	query3()

}
