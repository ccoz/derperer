# Derperer

Derperer 是一个围绕 Tailscale DERP 节点的整理与发布服务。当前版本会从本地 `result.json` 读取候选节点，构建 DERP Map，定期对节点进行带宽和时延检测，并通过 HTTP 接口对外提供筛选结果；也可以把带宽排序后的前 20 个节点写入指定 tailnet 的 ACL 配置。


## 主要能力

- 从 `DataPath/result.json` 逐行导入候选节点
- 解析节点主机名、IP、端口并构建自定义 DERP Map
- 使用 Tailscale 官方 `derp` 客户端对节点做测速
- 按周期复测节点并保存缓存
- 提供完整 DERP Map、排序结果和手动更新 Tailscale ACL 的 HTTP 接口
- 可选开启带 Basic Auth 的管理页面、monitor 和 pprof

## 工作流程

1. 服务启动时优先从 `DataPath/derp_map` 读取持久化缓存。
2. 如果缓存不存在，则从 `DataPath/result.json` 读取 JSON Lines 数据并构建节点。
3. 节点加入后会立即触发一次测速，后续按照 `DERPMapPolicy.RecheckInterval` 周期复测。
4. 服务会周期性重新读取 `result.json`，并可按配置自动调用 Tailscale API 更新 tailnet 配置。
5. 外部可以通过 `/derp.json`、`/derp_sort.json` 和 `/update` 获取结果或触发更新。

## 配置文件

程序使用 Viper 读取配置，默认会从以下位置查找 `derperer.yaml`：

- 当前工作目录下的 `./derperer.yaml`
- `$HOME/.derperer/derperer.yaml`

同时支持环境变量覆盖，前缀为 `DERPERER_`，并将配置中的 `.` 替换为 `_`。例如：

```bash
DERPERER_CONFIG_ADDRESS=:8081
DERPERER_CONFIG_DERPMAPPOLICY_RECHECKINTERVAL=30m
```

示例配置：

```yaml
config:
  AdminToken: ""
  Address: ":8080"
  DataPath: "/data"
  Account: ""
  ApiKey: ""
  FetchInterval: 6h0m0s
  DeleteInterval: 24h0m0s
  UpdateInterval: 4h0m0s
  DERPMapPolicy:
    BaselineBandwidth: 2
    CheckDuration: 10s
    RecheckInterval: 1h0m0s
    TestConcurrency: 1
```

配置项说明：

- `AdminToken`：设置后启用 `/admin` 管理入口，用户名固定为 `admin`，密码为该值
- `Address`：HTTP 服务监听地址
- `DataPath`：数据目录，服务会读取 `result.json`，并写入 `derp_map`、`last_fetch`、`last_fetch_tailscale`、`last_delete`
- `Account`：Tailscale tailnet 标识，用于调用 `/api/v2/tailnet/{account}/acl`
- `ApiKey`：Tailscale API Token
- `FetchInterval`：重新读取 `result.json` 的周期
- `UpdateInterval`：自动调用 Tailscale API 更新 tailnet ACL 配置的周期
- `DeleteInterval`：重置缓存并重新从 `result.json` 导入节点的周期
- `DERPMapPolicy.BaselineBandwidth`：用于计算 `HomeParams.RegionScore` 的基准带宽值
- `DERPMapPolicy.CheckDuration`：单次测速持续时间
- `DERPMapPolicy.RecheckInterval`：周期复测速率
- `DERPMapPolicy.TestConcurrency`：测速并发数

## 输入数据格式

`server` 当前依赖 `DataPath/result.json` 作为候选节点来源。该文件不是 JSON 数组，而是按行读取的 JSON Lines 文件，每行一个对象，例如：

```json
{"base_protocol":"https","as_organization":"Example-AS","city":"Shanghai","country":"CN","host":"https://derp.example.com:443","ip":"1.2.3.4","port":"443","region":"Shanghai"}
{"base_protocol":"https","as_organization":"Another-AS","city":"Beijing","country":"CN","host":"https://203.0.113.10:443","ip":"203.0.113.10","port":"443","region":"Beijing"}
```

注意：

- 建议 `host` 直接使用带协议的完整 URL，例如 `https://derp.example.com:443`
- 如果 `host` 没有携带协议头，程序会回退使用 `base_protocol` 来补全 URL 并判断是否作为 HTTPS DERP 节点导入
- 如果 `host` 是域名，程序会自动解析 IPv4/IPv6
- 相同 `host + port` 的节点会被跳过，避免重复导入

## 运行方式

### Docker Compose

仓库已提供 `docker-compose.yml`：

```bash
docker compose up -d
```

默认挂载关系如下：

- `./data:/data`
- `./derperer.yaml:/root/.derperer/derperer.yaml`

因此启动前至少需要准备：

- `./derperer.yaml`
- `./data/result.json`

### 二进制命令

CLI 入口包含以下子命令：

- `derperer server`：启动服务
- `derperer speedtest`：对指定 DERP Map 中的某个 region 做单独测速
- `derperer version`：输出版本号

`speedtest` 示例：

```bash
derperer speedtest \
  -u https://controlplane.tailscale.com/derpmap/default \
  -r tok \
  -d 30s
```

## HTTP 接口

### 公开接口

- `/`：简单索引页
- `/derp.json`：返回完整 DERP Map
- `/derp_sort.json`：返回按带宽排序后的前 20 个节点
- `/update`：按当前 Top 20 节点更新 Tailscale ACL 配置
- `/swagger/index.html`：Swagger UI

`/derp.json` 支持以下查询参数：

- `status=alive|error|unknown`，默认值为 `alive`
- `latency-limit=500ms`
- `bandwidth-limit=2Mbps`
- `all=true` 时忽略筛选条件并返回全部结果

说明：

- `/derp_sort.json` 返回的是排序后的简化结果，结构与 `/derp.json` 不完全相同，更接近 Tailscale ACL 里 `derpMap.Regions` 需要的格式
- `/update` 当前没有额外鉴权，只要服务可访问且配置了 `Account` 与 `ApiKey` 就可以触发更新，建议放在可信网络或反向代理保护后使用
- 当前实现提交到 Tailscale 的是整份 ACL 文档，而不只是 `derpMap`：其中包含代码内置的 `ACLs` 和 `SSH` 模板，请在使用前确认这正是你希望下发到 tailnet 的配置
- Swagger 文档已经与当前实现同步，字段和路由可以直接作为接口参考

### 管理接口

只有在配置了 `AdminToken` 后才会启用管理接口，认证方式为 HTTP Basic Auth：

- 用户名：`admin`
- 密码：`AdminToken` 的值

可用路由：

- `/admin`：管理入口页
- `/admin/monitor`：Fiber monitor
- `/admin/debug/pprof`：pprof
- `/admin/config`：查看当前配置
- `POST /admin/config`：修改进程内当前配置

说明：

- `POST /admin/config` 只会修改当前进程中的配置结构，不会回写 `derperer.yaml`

## 目录概览

- `cmd/`：CLI 入口和子命令定义
- `derperer/`：服务主体、DERP Map 构建、HTTP 路由、Tailscale API 调用
- `speedtest/`：DERP 节点测速实现
- `persistent/`：本地持久化
- `docs/`：Swagger 生成产物

## 当前实现的几个注意点

- `server` 启动流程当前依赖本地 `result.json`，不是实时从 FOFA 拉取数据
- `/update` 和自动更新会提交整份 ACL 文档，其中包含代码内置的 ACL/SSH 模板，部署前请务必确认其内容符合你的 tailnet 策略

## 参考

- Tailscale DERP: https://pkg.go.dev/tailscale.com/cmd/derper
- 原始项目说明参考: https://github.com/yoshino-s/derperer
