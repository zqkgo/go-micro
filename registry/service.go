package registry

// 服务发现对象
// 对于consul registry，获取的每个节点都是一个Service
// 对应的Nodes只有一个节点信息。例如一个service有10个节点就会获取
// 10个Service对象
type Service struct {
	Name      string            `json:"name"`
	Version   string            `json:"version"`
	Metadata  map[string]string `json:"metadata"`
	Endpoints []*Endpoint       `json:"endpoints"`
	Nodes     []*Node           `json:"nodes"`
}

// 服务节点
type Node struct {
	Id       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata"`
}

// Endpoint表示一个方法
type Endpoint struct {
	// 服务名.方法名
	Name     string            `json:"name"`
	Request  *Value            `json:"request"`
	Response *Value            `json:"response"`
	Metadata map[string]string `json:"metadata"`
}

// Value表示参数（变量）信息
type Value struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Values []*Value `json:"values"`
}
