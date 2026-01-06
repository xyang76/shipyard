package config

import "strconv"

type Config struct {
	Id          string   `json:"id"`
	Connection  string   `json:"connection"`
	Consistency string   `json:"consistency"`
	ClientPort  int      `json:"client"`
	Member      []Member `json:"members"`
	Replication []Node   `json:"replication"`
	Tree        Node     `json:"order"`
	Mode        string   `json:"mode"`
}

type Node struct {
	Group     string   `json:"group"`
	DCC       bool     `json:"dcc"`
	Children  []Node   `json:"children"`
	Peers     []string `json:"peers"`
	Observers []string `json:"observers"`
}

type Member struct {
	Id   string `json:"id"`
	Ip   string `json:"ip"`
	Type string `json:"type"`
}

func (c *Config) FindReplicateNode(group string) *Node {
	for _, child := range c.Replication {
		if child.Group == group {
			return &child
		}
	}
	return nil
}

func (c *Config) ContainsReplication(partition string) bool {
	for _, rep := range c.Replication {
		if rep.Group == partition {
			return true
		}
	}
	return false
}

func (c *Config) ContainsId(node *Node, id string) bool {
	if node.Peers != nil {
		for _, peer := range node.Peers {
			if peer == id {
				return true
			}
		}
	}
	if node.Observers != nil {
		for _, peer := range node.Observers {
			if peer == id {
				return true
			}
		}
	}
	return false
}

func (c *Config) FindOrderNode(group string) *Node {
	return dfsNode(c.Tree, group)
}

func (c *Config) FindChildren(node *Node) []string {
	rv := make([]string, 0, len(node.Children))

	for _, child := range node.Children {
		rv = append(rv, child.Group)
	}

	return rv
}

func dfsGroup(node Node, id string, rv []string) []string {
	if node.Peers != nil {
		for _, peer := range node.Peers {
			if peer == id {
				rv = append(rv, node.Group)
			}
		}
	}

	for _, child := range node.Children {
		rv = dfsGroup(child, id, rv)
	}
	return rv
}

func (c *Config) FindOrderGroup(id string) []string {
	return dfsGroup(c.Tree, id, []string{})
}

func (c *Config) FindMember(id string) *Member {
	for _, member := range c.Member {
		if member.Id == id {
			return &member
		}
	}
	return nil
}

func (c *Config) GetServicePort() string {
	return "0.0.0.0:" + strconv.Itoa(c.ClientPort)
}

func dfsNode(node Node, targetGroup string) *Node {
	if node.Group == targetGroup {
		return &node
	}

	for _, child := range node.Children {
		result := dfsNode(child, targetGroup)
		if result != nil {
			return result
		}
	}

	return nil
}

func dfsContainGroup(node Node, group string) bool {
	if node.Group == group {
		return true
	}
	for _, child := range node.Children {
		if dfsContainGroup(child, group) {
			return true
		}
	}
	return false
}

func dfsPartition(node Node, group string) (bool, []string) {
	for _, child := range node.Children {
		if child.Group == group {
			return true, []string{node.Group}
		} else {
			if v, str := dfsPartition(child, group); v {
				return true, append(str, node.Group)
			}
		}
	}
	return false, []string{}
}

func ToIPString(cfg Config) string {
	return "0.0.0.0:" + strconv.Itoa(cfg.ClientPort)
}
