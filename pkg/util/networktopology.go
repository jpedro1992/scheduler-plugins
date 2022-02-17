package util

import (
	hp "container/heap"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	v1alpha1 "sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
)

const (
	// netperf.p90.latency.milliseconds.origin.$n1.destination.$n2=$value\n";
	ConfigmapTemplate = "netperf.p90.latency.milliseconds.origin.%s.destination.%s"
)

// key for regions / zones concerning networkTopology Controller
type TopologyKey struct {
	Region string
	Zone   string
}

// key for zone / zone concerning networkTopology Controller
type ZoneKey struct {
	Z1 string
	Z2 string
}

// Graph and edge structures for shortest path
type edge struct {
	node string
	//region string
	//zone   string
	weight int
}

func GetConfigmapCostQuery(origin string, destination string) string {
	return fmt.Sprintf(ConfigmapTemplate, origin, destination)
}

// FindOriginBandwidthCapacity returns the bandwidth capacity of the given Origin to a certain destination
func FindOriginBandwidthCapacity(costList []v1alpha1.CostInfo, destination string) resource.Quantity {
	low := 0
	high := len(costList) - 1

	for low <= high {
		mid := (low + high) / 2
		if costList[mid].Destination == destination {
			return costList[mid].BandwidthCapacity // Return the Bandwidth Capacity
		} else if costList[mid].Destination < destination {
			low = mid + 1
		} else if costList[mid].Destination > destination {
			high = mid - 1
		}
	}
	// Bandwidth Capacity not found
	return resource.MustParse("0")
}

type Graph struct {
	nodes map[string][]edge
}

func NewGraph() *Graph {
	return &Graph{nodes: make(map[string][]edge)}
}

func (g *Graph) AddEdge(origin string, destiny string, weight int) { 				// regionOrigin string, zoneOrigin string, regionDestination string, zoneDestination string,
	g.nodes[origin] = append(g.nodes[origin], edge{node: destiny, weight: weight})  // region: regionDestination , zone: zoneDestination,
	g.nodes[destiny] = append(g.nodes[destiny], edge{node: origin, weight: weight}) // region: regionOrigin, zone: zoneOrigin,
}

func (g *Graph) RemoveEdge(origin string) {
	_, ok := g.nodes[origin]
	if ok {
		delete(g.nodes, origin)
	}
}

func (g *Graph) GetEdges(node string) []edge {
	return g.nodes[node]
}

func (g *Graph) GetEdgesRegion(node string) []edge {
	return g.nodes[node]
}

func (g *Graph) GetPath(origin string, destiny string) (int, []string) {
	h := newHeap()
	h.push(path{value: 0, nodes: []string{origin}})
	visited := make(map[string]bool)

	for len(*h.values) > 0 {
		// Find the nearest yet to visit node
		p := h.pop()
		node := p.nodes[len(p.nodes)-1]

		if visited[node] {
			continue
		}

		if node == destiny {
			return p.value, p.nodes
		}

		for _, e := range g.GetEdges(node) {
			if !visited[e.node] {
				// We calculate the total spent so far plus the cost and the path of getting here
				h.push(path{value: p.value + e.weight, nodes: append([]string{}, append(p.nodes, e.node)...)})
			}
		}

		visited[node] = true
	}

	return 0, nil
}

// heap-based structure for shortest path calculation
type path struct {
	value int
	nodes []string
}

type minPath []path

func (h minPath) Len() int           { return len(h) }
func (h minPath) Less(i, j int) bool { return h[i].value < h[j].value }
func (h minPath) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minPath) Push(x interface{}) {
	*h = append(*h, x.(path))
}

func (h *minPath) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type heap struct {
	values *minPath
}

func newHeap() *heap {
	return &heap{values: &minPath{}}
}

func (h *heap) push(p path) {
	hp.Push(h.values, p)
}

func (h *heap) pop() path {
	i := hp.Pop(h.values)
	return i.(path)
}
