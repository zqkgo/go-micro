package grpc

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/micro/go-micro/registry"
)

func extractValue(v reflect.Type, d int) *registry.Value {
	if d == 3 {
		return nil
	}
	if v == nil {
		return nil
	}

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	arg := &registry.Value{
		Name: v.Name(),
		Type: v.Name(),
	}

	switch v.Kind() {
	case reflect.Struct:
		// 如果是结构体则遍历每个成员
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			// 递归处理成员
			val := extractValue(f.Type, d+1)
			if val == nil {
				continue
			}

			// if we can find a json tag use it
			// 如果有json标签使用json字段名作为该字段名
			if tags := f.Tag.Get("json"); len(tags) > 0 {
				parts := strings.Split(tags, ",")
				if parts[0] == "-" || parts[0] == "omitempty" {
					continue
				}
				val.Name = parts[0]
			}

			// if there's no name default it
			if len(val.Name) == 0 {
				val.Name = v.Field(i).Name
			}

			arg.Values = append(arg.Values, val)
		}
	case reflect.Slice:
		p := v.Elem()
		if p.Kind() == reflect.Ptr {
			p = p.Elem()
		}
		arg.Type = "[]" + p.Name()
	}

	return arg
}

func extractEndpoint(method reflect.Method) *registry.Endpoint {
	// 非exported则略过
	if method.PkgPath != "" {
		return nil
	}

	var rspType, reqType reflect.Type
	var stream bool
	mt := method.Type
	// 方法的入参个数
	switch mt.NumIn() {
	case 3:
		reqType = mt.In(1) // 取第二个参数（请求对象）的类型
		rspType = mt.In(2) // 取第三个参数（响应对象）的类型
	case 4:
		reqType = mt.In(2)
		rspType = mt.In(3)
	default:
		return nil
	}

	// are we dealing with a stream?
	switch rspType.Kind() {
	case reflect.Func, reflect.Interface:
		stream = true
	}

	request := extractValue(reqType, 0)
	response := extractValue(rspType, 0)

	ep := &registry.Endpoint{
		Name:     method.Name,
		Request:  request,
		Response: response,
		Metadata: make(map[string]string),
	}

	if stream {
		ep.Metadata = map[string]string{
			"stream": fmt.Sprintf("%v", stream),
		}
	}

	return ep
}

// 从请求入参提取名称、类型、成员等信息，构造用于服务发现的参数格式
func extractSubValue(typ reflect.Type) *registry.Value {
	var reqType reflect.Type
	switch typ.NumIn() {
	case 1:
		reqType = typ.In(0)
	case 2:
		reqType = typ.In(1)
	case 3:
		reqType = typ.In(2)
	default:
		return nil
	}
	return extractValue(reqType, 0)
}
