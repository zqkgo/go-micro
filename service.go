package micro

import (
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/config/cmd"
	"github.com/micro/go-micro/debug/profile"
	"github.com/micro/go-micro/debug/profile/http"
	"github.com/micro/go-micro/debug/profile/pprof"
	"github.com/micro/go-micro/debug/service/handler"
	"github.com/micro/go-micro/plugin"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-micro/util/log"
	"github.com/micro/go-micro/util/wrapper"
)

type service struct {
	opts Options

	once sync.Once
}

func newService(opts ...Option) Service {
	// 拿到的opts实际上是若干Option类型的函数
	options := newOptions(opts...)

	// service name
	serviceName := options.Server.Options().Name

	// wrap client to inject From-Service header on any calls
	options.Client = wrapper.FromService(serviceName, options.Client)

	return &service{
		opts: options,
	}
}

func (s *service) Name() string {
	return s.opts.Server.Options().Name
}

// Init initialises options. Additionally it calls cmd.Init
// which parses command line flags. cmd.Init is only called
// on first Init.
// Init方法也会更新service持有的Options对象，所以传递给newService的参数
// 也可以传给Init方法。 因为很多选项（name, ttl, register interval...）
// 是设置为server、client等组件，所以要先设置组件，再设置选项才能生效。
func (s *service) Init(opts ...Option) {
	// process options
	for _, o := range opts {
		o(&s.opts)
	}
	s.once.Do(func() {
		// setup the plugins
		// 处理用逗号分割的插件路径
		// 插件？TODO: 需要看看Go内置的plugin包
		for _, p := range strings.Split(os.Getenv("MICRO_PLUGIN"), ",") {
			if len(p) == 0 {
				continue
			}

			// load the plugin
			c, err := plugin.Load(p)
			if err != nil {
				log.Fatal(err)
			}

			// initialise the plugin
			if err := plugin.Init(c); err != nil {
				log.Fatal(err)
			}
		}

		// Initialise the command flags, overriding new service
		// 将service的Options所持有的组件Broker、Registry、Transport等
		// 的 地址 作为参数 构造 可以设置cmd的Options对象 的函数。之所以传
		// 地址，是为了能够让Cmd对象根据传入的命令行参数 修改这些组件的实例。
		_ = s.opts.Cmd.Init(
			cmd.Broker(&s.opts.Broker),
			cmd.Registry(&s.opts.Registry),
			cmd.Transport(&s.opts.Transport),
			cmd.Client(&s.opts.Client),
			cmd.Server(&s.opts.Server),
		)
	})
}

func (s *service) Options() Options {
	return s.opts
}

func (s *service) Client() client.Client {
	return s.opts.Client
}

func (s *service) Server() server.Server {
	return s.opts.Server
}

func (s *service) String() string {
	return "micro"
}

func (s *service) Start() error {
	for _, fn := range s.opts.BeforeStart {
		if err := fn(); err != nil {
			return err
		}
	}

	if err := s.opts.Server.Start(); err != nil {
		return err
	}

	for _, fn := range s.opts.AfterStart {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (s *service) Stop() error {
	var gerr error

	for _, fn := range s.opts.BeforeStop {
		if err := fn(); err != nil {
			gerr = err
		}
	}

	if err := s.opts.Server.Stop(); err != nil {
		return err
	}

	for _, fn := range s.opts.AfterStop {
		if err := fn(); err != nil {
			gerr = err
		}
	}

	return gerr
}

func (s *service) Run() error {
	// register the debug handler
	s.opts.Server.Handle(
		s.opts.Server.NewHandler(
			handler.DefaultHandler,
			server.InternalHandler(true), // 标识注册到服务发现
		),
	)

	// start the profiler
	// TODO: set as an option to the service, don't just use pprof
	if prof := os.Getenv("MICRO_DEBUG_PROFILE"); len(prof) > 0 {
		var profiler profile.Profile

		// to view mutex contention
		runtime.SetMutexProfileFraction(5)
		// to view blocking profile
		runtime.SetBlockProfileRate(1)

		switch prof {
		case "http":
			profiler = http.NewProfile()
		default:
			service := s.opts.Server.Options().Name
			version := s.opts.Server.Options().Version
			id := s.opts.Server.Options().Id
			profiler = pprof.NewProfile(
				profile.Name(service + "." + version + "." + id),
			)
		}

		if err := profiler.Start(); err != nil {
			return err
		}
		defer profiler.Stop()
	}

	if err := s.Start(); err != nil {
		return err
	}

	ch := make(chan os.Signal, 1)
	if s.opts.Signal {
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	}

	select {
	// wait on kill signal
	case <-ch:
	// wait on context cancel
	case <-s.opts.Context.Done():
	}

	return s.Stop()
}
