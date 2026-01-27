package derperer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/monitor"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/gofiber/swagger"
	_ "github.com/koyangyang/derperer/docs"
	"github.com/koyangyang/derperer/fofa"
	"github.com/koyangyang/derperer/persistent"
	"github.com/sourcegraph/conc"
	"go.uber.org/zap"
)

type Derperer struct {
	config     DerpererConfig
	app        *fiber.App
	ctx        context.Context
	derpMap    *Map
	persistent *persistent.Persistent
}

type DerpererConfig struct {
	Address        string
	AdminToken     string
	FetchInterval  time.Duration
	DERPMapPolicy  DERPMapPolicy
	DataPath       string
	Account        string
	ApiKey         string
	UpdateInterval time.Duration
	DeleteInterval time.Duration
}

func NewDerperer(config DerpererConfig) (*Derperer, error) {
	app := fiber.New()
	ctx := context.Background()

	p, err := persistent.NewPersistent(config.DataPath)
	if err != nil {
		return nil, err
	}

	derperer := &Derperer{
		config:     config,
		app:        app,
		ctx:        ctx,
		derpMap:    NewMap(&config.DERPMapPolicy),
		persistent: p,
	}

	if err := derperer.persistent.Load("derp_map", derperer.derpMap); err != nil {
		zap.L().Warn("failed to load derp_map from persistent storage, will load from data/result.json", zap.Error(err))
		// 首次启动或数据丢失，立即从本地文件加载数据
		derperer.FetchFofaData()
	}

	app.Get("/swagger/*", swagger.HandlerDefault)

	app.Get("/", derperer.index)

	app.Get("/derp.json", derperer.getDerp)
	app.Get("/derp_sort.json", derperer.sortDerp)
	app.Get("/update", derperer.updateTailscale)

	if config.AdminToken != "" {
		adminApi := app.Group("/admin", basicauth.New(basicauth.Config{
			Users: map[string]string{
				"admin": config.AdminToken,
			},
		}))

		adminApi.Get("/", derperer.adminIndex)

		adminApi.Get("/monitor", monitor.New())
		adminApi.Use(pprof.New(pprof.Config{
			Prefix: "/admin",
		}))
		adminApi.Get("/config", derperer.getConfig)
		adminApi.Post("/config", derperer.setConfig)
	}

	return derperer, nil
}

// LocalDataRecord 本地数据文件的记录结构
type LocalDataRecord struct {
	BaseProtocol   string `json:"base_protocol"`
	ASOrganization string `json:"as_organization"`
	City           string `json:"city"`
	Country        string `json:"country"`
	Host           string `json:"host"`
	IP             string `json:"ip"`
	Port           string `json:"port"`
	Region         string `json:"region"`
}

// ToFofaResult 转换为 FofaResult 格式
func (r *LocalDataRecord) ToFofaResult() fofa.FofaResult {
	// 从 host 中推断 protocol
	protocol := "http"
	if strings.HasPrefix(r.Host, "https://") {
		protocol = "https"
	}

	return fofa.FofaResult{
		IP:             r.IP,
		Port:           r.Port,
		Host:           r.Host,
		Protocol:       protocol,
		Country:        r.Country,
		Region:         r.Region,
		City:           r.City,
		ASOrganization: r.ASOrganization,
	}
}

func (d *Derperer) LoadDataFromFile() error {
	logger := zap.L()
	filePath := filepath.Join(d.config.DataPath, "result.json")
	logger.Info("loading data from file", zap.String("path", filePath))

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var record LocalDataRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			logger.Error("failed to parse json line", zap.Error(err), zap.String("line", scanner.Text()))
			continue
		}

		fofaResult := record.ToFofaResult()
		if err := d.derpMap.AddFofaResult(fofaResult); err != nil {
			logger.Debug("failed to add result", zap.Error(err), zap.String("host", record.Host))
			continue
		}
		count++

		// 定期保存进度
		if count%10 == 0 {
			if err := d.persistent.Save("derp_map", d.derpMap); err != nil {
				logger.Error("failed to save derp_map", zap.Error(err))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	// 最终保存
	if err := d.persistent.Save("derp_map", d.derpMap); err != nil {
		logger.Error("failed to save derp_map", zap.Error(err))
	}

	logger.Info("finished loading data from file", zap.Int("count", count))
	return nil
}

func (d *Derperer) RemoveFofaData() {
	logger := zap.L()
	logger.Info("removing data")
	if err := d.persistent.Delete("derp_map"); err != nil {
		logger.Error("failed to delete derp_map", zap.Error(err))
	}
	d.derpMap = NewMap(&d.config.DERPMapPolicy)
	d.FetchFofaData()
}

func (d *Derperer) FetchFofaData() {
	logger := zap.L()
	logger.Info("loading data from local file")
	if err := d.LoadDataFromFile(); err != nil {
		logger.Error("failed to load data from file", zap.Error(err))
	}
}

func (d *Derperer) Start() {
	wg := conc.WaitGroup{}

	wg.Go(d.derpMap.Recheck)

	wg.Go(func() {
		for {
			var lastFetch time.Time
			if err := d.persistent.Load("last_fetch", &lastFetch); err != nil {
				zap.L().Error("failed to load last_fetch", zap.Error(err))
			}

			<-time.After(d.config.FetchInterval - time.Since(lastFetch))
			d.FetchFofaData()

			if err := d.persistent.Save("last_fetch", time.Now()); err != nil {
				zap.L().Error("failed to save last_fetch", zap.Error(err))
			}
		}
	})

	wg.Go(func() {
		for {
			var lastFetch time.Time
			if err := d.persistent.Load("last_fetch_tailscale", &lastFetch); err != nil {
				zap.L().Error("failed to load last_fetch_tailscale", zap.Error(err))
			}

			<-time.After(d.config.UpdateInterval - time.Since(lastFetch))
			d.autoupdateTailscale()

			if err := d.persistent.Save("last_fetch_tailscale", time.Now()); err != nil {
				zap.L().Error("failed to save last_fetch_tailscale", zap.Error(err))
			}
		}
	})

	wg.Go(func() {
		for {
			var lastFetch time.Time
			if err := d.persistent.Load("last_delete", &lastFetch); err != nil {
				zap.L().Error("failed to load last_delete", zap.Error(err))
			}

			<-time.After(d.config.FetchInterval - time.Since(lastFetch))
			d.RemoveFofaData()

			if err := d.persistent.Save("last_delete", time.Now()); err != nil {
				zap.L().Error("failed to save last_delete", zap.Error(err))
			}
		}
	})

	wg.Go(func() {
		d.app.Listen(d.config.Address)
	})

	wg.Wait()
}

// @Summary Index
// @Produce html
// @Router / [get]
func (d *Derperer) index(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/html")
	return c.SendString(strings.TrimSpace(`
<a href="/derp.json">derp.json</a><br/>
<a href="/swagger/index.html">swagger</a><br/>
<a href="/admin">admin</a><br/>
		`))
}

// @Summary Get DERP Map
// @Param status query string false "alive|error|all" Enums(alive, error, all)
// @Param latency-limit query string false "latency limit, e.g. 500ms"
// @Param bandwidth-limit query string string "bandwidth limit, e.g. 2Mbps"
// @Produce json
// @Router /derp.json [get]
func (d *Derperer) getDerp(c *fiber.Ctx) error {
	var filter DERPMapFilter
	if err := c.QueryParser(&filter); err != nil {
		return err
	}
	m, err := d.derpMap.FilterDERPMap(filter)
	if err != nil {
		return err
	}
	return c.JSON(m)
}

func (d *Derperer) sortDerp(c *fiber.Ctx) error {
	m, err := d.derpMap.SortTopKDERPMap(20)
	if err != nil {
		return err
	}
	return c.JSON(m)
}

func (d *Derperer) updateTailscale(c *fiber.Ctx) error {
	m, err := d.derpMap.SortTopKDERPMap(20)
	if err != nil {
		return err
	}
	t := UpdateACL(m.Regions, d.config.Account, d.config.ApiKey)
	return c.JSON(t)
}

func (d *Derperer) autoupdateTailscale() {
	m, err := d.derpMap.SortTopKDERPMap(20)
	if err != nil {
		return
	}
	t := UpdateACL(m.Regions, d.config.Account, d.config.ApiKey)
	fmt.Println(t)
}

// @securityDefinitions.basic BasicAuth

// @Summary Admin Index
// @Produce html
// @Security BasicAuth
// @Router /admin [get]
func (d *Derperer) adminIndex(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/html")
	return c.SendString(`
	<a href="/admin/monitor">monitor</a><br/>
	<a href="/admin/debug/pprof">pprof</a><br/>
	<a href="/admin/config">config</a> or <code>POST</code> to change config <br/>
	`)
}

// @Summary Get Server Config
// @Produce json
// @Security BasicAuth
// @Router /admin/config [get]
func (d *Derperer) getConfig(c *fiber.Ctx) error {
	return c.JSON(d.config)
}

// @Summary Change Server Config
// @Accept json
// @Param config body derperer.DerpererConfig true "config"
// @Produce json
// @Security BasicAuth
// @Router /admin/config [post]
func (d *Derperer) setConfig(c *fiber.Ctx) error {
	if err := c.BodyParser(&d.config); err != nil {
		return err
	}
	return c.JSON(d.config)
}
