package main

import (
	"bufio"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/yunify/qingstor-sdk-go/config"
	qs "github.com/yunify/qingstor-sdk-go/service"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
)

/*
	此程序的目的是用于向青云对象存储，比对同步数据
*/
var (
	AccessKey  string
	SecretKey  string
	BucketName string
	Zone       string
	Source     string
	Target     string
	Bucket     *qs.Bucket
)

func init() {

	pflag.String("source", "/var/tmp/new_dir3", "source file path")
	pflag.String("target", "testBackup", "target in qingstor")
	pflag.String("config", "", "config file")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	Source = viper.GetString("source")
	Target = viper.GetString("target")

	log.Println("Source", Source)
	log.Println("Target", Target)
	configfile := viper.GetString("config")
	if len(configfile) > 0 {
		viper.SetConfigFile(configfile)
	} else {
		viper.AddConfigPath(".")
		fname := "sync_qingstor"
		viper.SetConfigName(fname)
	}

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		log.Println("can't read config file")
		panic(err)
	}

	AccessKey = viper.GetString("access_key")
	SecretKey = viper.GetString("secret_key")
	BucketName = viper.GetString("bucket_name")
	Zone = viper.GetString("zone")

	// 初始化bucket
	configuration, _ := config.New(AccessKey, SecretKey)
	qsService, _ := qs.Init(configuration)
	Bucket, _ = qsService.Bucket(BucketName, Zone)
	log.Println("starting...")
}

func scanSource(dirPth string) int {
	// 1. 扫描本地文件
	name := "/tmp/sourceFileList.txt"
	f, err := os.Create(name)
	defer f.Close()

	if err != nil {
		log.Println("create file error", err)
		return -1
	}
	var count int = 0

	err = GetAllFiles(dirPth, &count, f)
	if err != nil {
		log.Println(err)
	}
	log.Println("total get file", count)
	return count
}

//获取指定目录下的所有文件,包含子目录下的文件
func GetAllFiles(dirPth string, count *int, fp *os.File) (err error) {
	var dirs []string
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return err
	}

	PthSep := string(os.PathSeparator)

	for _, fi := range dir {
		if fi.IsDir() { // 目录, 递归遍历
			dirs = append(dirs, dirPth+PthSep+fi.Name())
			GetAllFiles(dirPth+PthSep+fi.Name(), count, fp)
		} else {
			*count++
			fp.WriteString(strconv.Itoa(*count) + ":" + dirPth + PthSep + fi.Name())
			fp.Write([]byte{'\n'})
		}
	}

	return nil
}

func diff(source, target string) int {
	name := "/tmp/sourceFileList.txt"
	fp, err := os.Open(name)
	defer fp.Close()
	if err != nil {
		log.Println("open sourceFileList error", err)
		return -1
	}

	// 记录需要上传的文件
	nameUpload := "/tmp/uploadFileList.txt"
	uploadfile, err := os.Create(nameUpload)
	defer uploadfile.Close()

	if err != nil {
		log.Println("create uploadFileList error", err)
		return -1
	}

	br := bufio.NewReader(fp)
	count := 0
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		line := string(a)
		itemList := strings.Split(line, ":")
		log.Println("diff", "current:", itemList[0], "file:", itemList[1])

		if check(source, itemList[1], target) {
			count++
			log.Println("wait to upload ", itemList[1])
			uploadfile.WriteString(strconv.Itoa(count) + ":" + itemList[1])
			uploadfile.Write([]byte{'\n'})
		}
	}
	return count
}

// 检查是否要上传
// false 不需要上传
// true 需要上传
func check(source string, filepath string, target string) bool {
	key := path.Join(target, filepath[len(source):])
	log.Println("check", "filepath:", filepath, "key:", key)
	remote, err := Bucket.HeadObject(key, nil)
	if err != nil && strings.Index(err.Error(), "404") != -1 {
		return true
	} else if err != nil {
		log.Println("Bucket.HeadObject error", err)
		return false
	}

	info, err := os.Stat(filepath)
	if err != nil {
		log.Println("os.Stat error", err)
		return false
	}
	localSize := info.Size()
	localLastModified := info.ModTime()
	if localSize != *remote.ContentLength && localLastModified.After(*remote.LastModified) {
		return true
	}
	return false
}

func upload(source string, target string) {
	name := "/tmp/uploadFileList.txt"
	fp, err := os.Open(name)
	defer fp.Close()
	if err != nil {
		log.Println("open uploadFileList error", err)
		return
	}

	br := bufio.NewReader(fp)
	successCount := 0
	failedCount := 0
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		line := string(a)
		itemList := strings.Split(line, ":")
		filepath := itemList[1]
		log.Println("upload", "current:", itemList[0], "file:", filepath)
		key := path.Join(target, filepath[len(source):])
		result := uploadFile(filepath, key)
		if result {
			successCount++
		} else {
			failedCount++
		}
	}
	log.Println("upload", "successCount:", successCount, "failedCount", failedCount)
}

func uploadFile(filepath, key string) bool {
	// Open file
	var file *os.File
	file, err := os.Open(filepath)
	if err != nil {
		log.Println("uploadFile--open file error", filepath)
		return false
	}
	defer file.Close()

	// Put object
	oOutput, err := Bucket.PutObject(key, &qs.PutObjectInput{Body: file})

	if qs.IntValue(oOutput.StatusCode) == http.StatusCreated {
		// Print the HTTP status code.
		// Example: 201
		return true
	} else if err != nil {
		// Example: QingStor Error: StatusCode 403, Code "permission_denied"...
		log.Println("uploadFile--upload error", filepath, err)
		return false
	}
	log.Println(oOutput, err)
	return false
}

func main() {
	log.Println("----start----")
	// 1. 扫描本地文件
	log.Println("-------1. scanSource-----")
	scanSource(Source)
	// 2. 逐条比对对象存储中的文件，判断需要上传的数量
	log.Println("-------2. diff -----")
	count := diff(Source, Target)
	log.Println("-------2. upload -----", "needToUpload", count)
	if count <= 0 {
		log.Println("no need to upload, ----end----")
		return
	}
	upload(Source, Target)
	log.Println("----end----")
}
