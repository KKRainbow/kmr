package cli

import (
	"fmt"
	"github.com/naturali/kmr/config"
	"github.com/naturali/kmr/jobgraph"
	"github.com/urfave/cli"
	"io/ioutil"
	"k8s.io/client-go/rest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"github.com/naturali/kmr/worker"
	"path"
	"github.com/naturali/kmr/util/log"
	"encoding/json"
	"errors"
	"github.com/naturali/kmr/util"
	"math/rand"
	"github.com/naturali/kmr/bucket"
	"net"
	"github.com/naturali/kmr/master"
	"github.com/naturali/kmr/executor"
)

func loadBuckets(m, i, r *config.BucketDescription) ([]bucket.Bucket, error) {
	mapBucket, err := bucket.NewBucket(m.BucketType, m.Config)
	if err != nil {
		return nil, err
	}
	interBucket, err := bucket.NewBucket(i.BucketType, i.Config)
	if err != nil {
		return nil, err
	}
	reduceBucket, err := bucket.NewBucket(r.BucketType, r.Config)
	if err != nil {
		return nil, err
	}

	return []bucket.Bucket{
		mapBucket, interBucket, reduceBucket,
	}, nil
}

func loadBucketsFromRemote(conf *config.RemoteConfig) ([]bucket.Bucket, error) {
	buckets, err := loadBuckets(conf.MapBucket, conf.InterBucket, conf.ReduceBucket)
	return buckets, err
}

func loadBucketsFromLocal(conf *config.LocalConfig) ([]bucket.Bucket, error) {
	buckets, err := loadBuckets(conf.MapBucket, conf.InterBucket, conf.ReduceBucket)
	return buckets, err
}

func Run(job *jobgraph.Job) {
	if len(job.GetName()) == 0 {
		job.SetName("anonymous-kmr-job")
	}

	var err error

	repMap := map[string]string{
		"${JOBNAME}": job.GetName(),
	}
	var conf *config.KMRConfig

	var buckets []bucket.Bucket
	var assetFolder string

	app := cli.NewApp()
	app.Name = job.GetName()
	app.Description = "A KMR application named " + job.GetName()
	app.Author = "Naturali"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:  "config",
			Usage: "Load config from `FILES`",
		},
		cli.Int64Flag{
			Name:   "random-seed",
			Value:  time.Now().UnixNano(),
			Usage:  "Used to synchronize information between workers and master",
			Hidden: true,
		},
		cli.StringFlag{
			Name:  "asset-folder",
			Value: "./assets",
			Usage: "Files under asset folder will be packed into docker image. " +
				"KMR APP can use this files as they are under './'",
		},
	}
	app.Before = func(c *cli.Context) error {
		conf = config.LoadConfigFromMultiFiles(repMap, append(config.GetConfigLoadOrder(), c.StringSlice("config")...)...)
		job.ValidateGraph()

		assetFolder, err = filepath.Abs(c.String("asset-folder"))
		if err != nil {
			return err
		}
		if f, err := os.Stat(assetFolder); err != nil || !f.IsDir() {
			if os.IsNotExist(err) {
				err := os.MkdirAll(assetFolder, 0777)
				if err != nil {
					return cli.NewMultiError(err)
				}
				originAfter := app.After
				app.After = func(ctx *cli.Context) (err error) {
					err = nil
					if originAfter != nil {
						err = originAfter(ctx)
					}
					os.RemoveAll(assetFolder)
					return
				}
			} else {
				return cli.NewExitError("Asset folder "+assetFolder+" is incorrect", 1)
			}
		}
		log.Info("Asset folder is", assetFolder)

		return nil
	}

	app.Commands = []cli.Command{
		{
			Name: "master",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "remote",
					Usage: "Run on kubernetes",
				},
				cli.IntFlag{
					Name:  "port",
					Value: 50051,
					Usage: "Define how many workers",
				},
				cli.IntFlag{
					Name:  "worker-num",
					Value: 1,
					Usage: "Define how many workers",
				},
				cli.BoolFlag{
					Name:  "listen-only",
					Usage: "Listen and waiting for workers",
				},
				cli.IntFlag{
					Name:  "cpu-limit",
					Value: 1,
					Usage: "Define worker cpu limit when run in k8s",
				},
				cli.StringFlag{
					Name:   "image-name",
					Usage:  "Worker docker image name used in remote mode",
					Hidden: true,
				},
				cli.StringFlag{
					Name:   "service-name",
					Usage:  "k8s service name used in remote mode",
					Hidden: true,
				},
			},
			Action: func(ctx *cli.Context) error {
				var workerCtl worker.WorkerCtl
				seed := ctx.GlobalInt64("random-seed")
				if ctx.Bool("remote") {
					var err error
					buckets,err = loadBucketsFromRemote(conf.Remote)

					var k8sconfig *rest.Config

					k8sSchema := os.Getenv("KUBERNETES_API_SCHEMA")
					if k8sSchema != "http" { // InCluster
						k8sconfig, err = rest.InClusterConfig()
						if err != nil {
							log.Fatalf("Can't get incluster config, %v", err)
						}
					} else { // For debug usage. > source dev_environment.sh
						host := os.Getenv("KUBERNETES_SERVICE_HOST")
						port := os.Getenv("KUBERNETES_SERVICE_PORT")

						k8sconfig = &rest.Config{
							Host: fmt.Sprintf("%s://%s", k8sSchema, net.JoinHostPort(host, port)),
						}
						token := os.Getenv("KUBERNETES_API_ACCOUNT_TOKEN")
						if len(token) > 0 {
							k8sconfig.BearerToken = token
						}
					}
					exe, err1 := os.Executable()
					exe, err2 := filepath.EvalSymlinks(exe)
					if err1 != nil || err2 != nil {
						exe = os.Args[0]
						log.Error("Cannot use os.Executable to determine executable path, use", exe, "instead")
					}
					workerCtl = worker.NewK8sWorkerCtl(&worker.K8sWorkerConfig{
						Name:         job.GetName(),
						CPULimit:     "1",
						Namespace:    *conf.Remote.Namespace,
						K8sConfig:    *k8sconfig,
						WorkerNum:    ctx.Int("worker-num"),
						Volumes:      *conf.Remote.PodDesc.Volumes,
						VolumeMounts: *conf.Remote.PodDesc.VolumeMounts,
						RandomSeed:   seed,
						Image:        ctx.String("image-name"),
						Command: []string{
							exe,
							"--config", strings.Join([]string(ctx.GlobalStringSlice("config")), ","),
							"worker",
							"--master-addr", fmt.Sprintf("%v:%v", ctx.String("service-name"), ctx.Int("port")),
						},
					})
				} else {
					buckets, err = loadBucketsFromLocal(conf.Local)
					workerCtl = worker.NewLocalWorkerCtl(job, ctx.Int("port"), 64, buckets)
					os.Chdir(assetFolder)
				}

				if ctx.Bool("listen-only") {
					workerCtl = nil
				}

				if workerCtl != nil {
					workerCtl.StartWorkers(ctx.Int("worker-num"))
				}

				m := master.NewMaster(job, strconv.Itoa(ctx.Int("port")), buckets[0], buckets[1], buckets[2])
				m.Run()
				return nil
			},
		},
		{
			Name: "worker",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "master-addr",
					Value: "localhost:50051",
					Usage: "Master's address, format: \"HOSTNAME:PORT\"",
				},
				cli.BoolFlag{
					Name:  "local",
					Usage: "This worker will read local config",
				},
			},
			Usage: "Run in remote worker mode, this will read remote config items",
			Action: func(ctx *cli.Context) error {
				randomSeed := ctx.GlobalInt64("random-seed")
				rand.Seed(randomSeed)
				workerID := rand.Int63()

				if ctx.Bool("local") {
					buckets,err = loadBucketsFromLocal(conf.Local)
					os.Chdir(assetFolder)
				} else {
					buckets,err = loadBucketsFromRemote(conf.Remote)
				}
				w := executor.NewWorker(job, workerID, ctx.String("master-addr"), 64, buckets[0],buckets[1], buckets[2])
				w.Run()
				return nil
			},
		},
		{
			Name: "deploy",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "port",
					Value: 50051,
					Usage: "Define how many workers",
				},
				cli.IntFlag{
					Name:  "worker-num",
					Value: 1,
					Usage: "Define how many workers",
				},
				cli.IntFlag{
					Name:  "cpu-limit",
					Value: 1,
					Usage: "Define worker cpu limit when run in k8s",
				},
				cli.StringSliceFlag{
					Name: "image-tags",
				},
			},
			Usage: "Deploy KMR Application in k8s",
			Action: func(ctx *cli.Context) error {
				dockerWorkDir := "/kmrapp"
				// collect files under current folder
				configFilePath := path.Join(assetFolder, "internal-used-config.json")
				executablePath := path.Join(assetFolder, "internal-used-executable")

				if _, err := os.Stat(configFilePath); err == nil {
					return cli.NewExitError(configFilePath+"should not exists", 1)
				}
				if _, err := os.Stat(executablePath); err == nil {
					return cli.NewExitError(executablePath+"should not exists", 1)
				}

				configJson, err := json.MarshalIndent(conf, "", "\t")
				if err != nil {
					return err
				}
				err = ioutil.WriteFile(configFilePath, configJson, 0666)
				defer os.Remove(configFilePath)
				if err != nil {
					return err
				}

				//TODO Use Ermine to pack executable and dynamic library
				exe, err1 := os.Executable()
				exe, err2 := filepath.EvalSymlinks(exe)
				if err1 != nil || err2 != nil {
					return cli.NewMultiError(errors.New("cannot locate executable"), err1, err2)
				}
				os.Link(exe, executablePath)
				defer os.Remove(executablePath)

				tags := ctx.StringSlice("image-tags")
				if len(tags) == 0 {
					tags = append(tags, strings.ToLower(job.GetName())+":"+"latest")
				}

				files, err := filepath.Glob(assetFolder + "/*")
				if err != nil {
					return err
				}

				fmt.Println(files)
				imageName, err := util.CreateDockerImage(assetFolder, *conf.Remote.DockerRegistry, tags, files, dockerWorkDir)
				if err != nil {
					return err
				}

				pod, service, err := util.CreateK8sKMRJob(job.GetName(),
					*conf.Remote.ServiceAccount,
					*conf.Remote.Namespace,
					*conf.Remote.PodDesc, imageName, dockerWorkDir,
					[]string{dockerWorkDir + "/internal-used-executable", "--config", dockerWorkDir + "/internal-used-config.json",
						"master",
						"--remote", "--port", fmt.Sprint(ctx.Int("port")),
						"--worker-num", fmt.Sprint(ctx.Int("worker-num")),
						"--cpu-limit", fmt.Sprint(ctx.Int("cpu-limit")),
						"--image-name", imageName,
						"--service-name", job.GetName(),
					},
					int32(ctx.Int("port")))

				if err != nil {
					return cli.NewMultiError(err)
				}
				log.Info("Pod: ", pod, "Service: ", service)
				return nil
			},
		},
	}

	app.Run(os.Args)
	os.Exit(0)
}
