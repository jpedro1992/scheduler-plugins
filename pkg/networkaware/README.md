# Overview

This folder holds the NetworkAware plugins implemented as discussed in the [KEP - Network-Aware framework](https://github.com/kubernetes-sigs/scheduler-plugins/pull/282).

## Maturity Level

- [x] 💡 Sample (for demonstrating and inspiring purpose)
- [ ] 👶 Alpha (used in companies for pilot projects)
- [ ] 👦 Beta (used in companies and developed actively)
- [ ] 👨 Stable (used in companies for production workloads)

## TopologicalSort Plugin (QueueSort)

The `TopologicalSort` **QueueSort** plugin orders pods to be scheduled in an [**AppGroup**](../../manifests/appgroup/crd.yaml) based on their 
microservice dependencies related to [TopologicalSort](https://en.wikipedia.org/wiki/Topological_sorting).

The `TopologicalSort` plugin favors topological sorting over prioritized and QoS sorting. 
This plugin is an additional sorting plugin created for the network-aware framework. 
If your application/use case favors priority sorting, please enable the default or QoS plugin. 
This plugin supports topology sorting for AppGroups and follows the strategy of QoS Sort for pods belonging to different AppGroups. 

Further details and examples are described [here](../networkaware/topologicalsort).

## NetworkOverhead Plugin (Filter & Score)

The `NetworkOverhead` **Filter & Score** plugin filters out nodes based on microservice dependencies 
defined in an **AppGroup** and scores nodes with lower network costs (described in a [**NetworkTopology**](../../manifests/networktopology/crd.yaml))
higher to achieve latency-aware scheduling.

Further details and examples are described [here](../networkaware/networkoverhead). 

## Scheduler Config example 

Consider the following scheduler config as an example to enable both plugins:

```yaml
apiVersion: kubescheduler.config.k8s.io/v1beta2
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: false
clientConnection:
  kubeconfig: "REPLACE_ME_WITH_KUBE_CONFIG_PATH"
profiles:
- schedulerName: network-aware-scheduler
  plugins:
    queueSort:
      enabled:
      - name: TopologicalSort
      disabled:
      - name: "*"
    filter:
      enabled:
      - name: NetworkOverhead
    score:
      disabled: # Preferably avoid the combination of NodeResourcesFit with NetworkOverhead
      - name: NodeResourcesFit
      enabled: # A higher weight is given to NetworkOverhead to favor allocation schemes with lower latency.
      - name: NetworkOverhead
        weight: 5
      - name: BalancedAllocation
        weight: 1
  pluginConfig:
  - name: TopologicalSort
    args:
      namespaces:
      - "default"
  - name: NetworkOverhead
    args:
      namespaces:
      - "default"
      weightsName: "UserDefined" # weights applied by the plugin
      networkTopologyName: "net-topology-test" # networkTopology CR used by the plugin
```

## Deployment Example based on [Online Boutique](https://github.com/GoogleCloudPlatform/microservices-demo)

### Kubernetes cluster with topology Labels

Typically, the kubelet or the external cloud-controller-manager populates Kubernetes topology labels with the
information provided by the cloud provider. This will be set only if you are using a cloud provider. 
However, you should consider setting topology labels on nodes if it makes sense in your network topology 
and for full applicability of the network-aware framework. Further details about topology 
labels are given [here](https://kubernetes.io/docs/reference/labels-annotations-taints/#topologykubernetesioregion).

As a networkTopology CR example, see the following YAML file:

```yaml
# Example Network CRD 
apiVersion: scheduling.sigs.k8s.io/v1alpha1
kind: NetworkTopology
metadata:
  name: net-topology-test
  namespace: default
spec:
  configMapName: "netperfMetrics"
  weights:
    # Region label: "topology.kubernetes.io/region"
    # Zone Label:   "topology.kubernetes.io/zone"
    # 2 Regions:  us-west-1
    #             us-east-1
    # 4 Zones:    us-west-1: z1, z2
    #             us-east-1: z3, z4
    - name: "UserDefined"
      costList: # Define weights between regions or between zones 
        - topologyKey: "topology.kubernetes.io/region" # region costs
          originCosts:
            - origin: "us-west-1"
              costs:
                - destination: "us-east-1"
                  bandwidthCapacity: "10Gi"
                  networkCost: 20
            - origin: "us-east-1"
              costs:
                - destination: "us-west-1"
                  bandwidthCapacity: "10Gi"
                  networkCost: 20
        - topologyKey: "topology.kubernetes.io/zone" # zone costs
          originCosts:
            - origin: "z1"
              costs:
                - destination: "z2"
                  bandwidthCapacity: "1Gi"
                  networkCost: 5
            - origin: "z2"
              costs:
                - destination: "z1"
                  bandwidthCapacity: "1Gi"
                  networkCost: 5
            - origin: "z3"
              costs:
                - destination: "z4"
                  bandwidthCapacity: "1Gi"
                  networkCost: 10
            - origin: "z4"
              costs:
                - destination: "z3"
                  bandwidthCapacity: "1Gi"
                  networkCost: 10
```

The topology corresponds to two regions `us-west-1` and `us-east-1`, and four zones `z1 - z4`.
Let's consider that your cluster owns four nodes. Topology labels can be submitted as follows:

```powershell
# region labels
kubectl label nodes n1 topology.kubernetes.io/region=us-west-1 (--overwrite flag overwrites previous defined labels)
kubectl label nodes n2 topology.kubernetes.io/region=us-west-1 
kubectl label nodes n3 topology.kubernetes.io/region=us-east-1 
kubectl label nodes n4 topology.kubernetes.io/region=us-east-1

# zone labels
kubectl label nodes n1 topology.kubernetes.io/zone=z1 
kubectl label nodes n2 topology.kubernetes.io/zone=z2 
kubectl label nodes n3 topology.kubernetes.io/zone=z3 
kubectl label nodes n4 topology.kubernetes.io/zone=z4
```

These labels help the network-aware plugins to find nodes with low costs for pod deployments based on AppGroups.

### AppGroup deployment

An example of an AppGroup CR for the online Boutique application is available [here](../../manifests/appgroup/onlineBoutique-appGroup-example.yaml).

Then, pods belonging to this AppGroup can be deployed in the cluster as shown below:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: adservice
spec:
  replicas: 3
  selector:
    matchLabels:
      app: adservice
      workload: adservice
  template:
    metadata:
      labels:
        app-group.scheduling.sigs.k8s.io: online-boutique
        app: adservice
        workload: adservice
    spec:
      schedulerName: network-aware-scheduler
      terminationGracePeriodSeconds: 5
      tolerations:
      nodeSelector:
      initContainers:
        - name: sfx-instrumentation
          image: quay.io/signalfuse/sfx-zero-config-agent:latest
          # image: sfx-zero-config-agent
          # imagePullPolicy: Never
          volumeMounts:
            - mountPath: /opt/sfx/
              name: sfx-instrumentation
      containers:
        - name: server
          image: quay.io/signalfuse/microservices-demo-adservice:433c23881a
          ports:
            - containerPort: 9555
          env:
            - name: PORT
              value: '9555'
            - name: OTEL_EXPORTER_ZIPKIN_SERVICE_NAME
              value: adservice
            - name: NODE_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.hostIP
            - name: OTEL_EXPORTER
              value: zipkin
            - name: JAVA_TOOL_OPTIONS
              value: -javaagent:/opt/sfx/splunk-otel-javaagent-all.jar
            - name: OTEL_EXPORTER_ZIPKIN_ENDPOINT
              value: 'http://$(NODE_IP):9411/v1/trace'
          volumeMounts:
            - mountPath: /opt/sfx
              name: sfx-instrumentation
          resources:
            requests:
              cpu: 200m
              memory: 180Mi
            limits:
              cpu: 300m
              memory: 300Mi
          readinessProbe:
            initialDelaySeconds: 60
            periodSeconds: 25
            exec:
              command: ['/bin/grpc_health_probe', '-addr=:9555']
          livenessProbe:
            initialDelaySeconds: 60
            periodSeconds: 30
            exec:
              command: ['/bin/grpc_health_probe', '-addr=:9555']
      volumes:
        - emptyDir: {}
          name: sfx-instrumentation
---
apiVersion: v1
kind: Service
metadata:
  name: adservice
spec:
  type: ClusterIP
  selector:
    app: adservice
  ports:
    - name: grpc
      port: 9555
      targetPort: 9555
---

```

The network-aware plugins will favor nodes with lower costs based on the deployed AppGroup. 

## Summary

Further details about the network-aware framework are available [here](../../kep/260-network-aware-scheduling/README.md).