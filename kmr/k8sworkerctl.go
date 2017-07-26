package kmr

type K8sWorkerConfig struct {
	CPULimit   int
	Command    string
	Image      string
	DetailDesc string
}
