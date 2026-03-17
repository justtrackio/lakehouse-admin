package internal

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	buildassets "github.com/justtrackio/lakehouse-admin/build"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

type SparkApplicationManifest struct {
	APIVersion string                       `yaml:"apiVersion" json:"apiVersion"`
	Kind       string                       `yaml:"kind" json:"kind"`
	Metadata   SparkApplicationMetadata     `yaml:"metadata" json:"metadata"`
	Spec       SparkApplicationManifestSpec `yaml:"spec" json:"spec"`
	Status     SparkApplicationStatus       `yaml:"status" json:"status"`
}

type sparkApplicationCreateManifest struct {
	APIVersion string                       `json:"apiVersion"`
	Kind       string                       `json:"kind"`
	Metadata   SparkApplicationMetadata     `json:"metadata"`
	Spec       SparkApplicationManifestSpec `json:"spec"`
}

type SparkApplicationStatus struct {
	ApplicationState       SparkApplicationState            `yaml:"applicationState" json:"applicationState"`
	CurrentState           SparkApplicationState            `yaml:"currentState" json:"currentState"`
	StateTransitionHistory map[string]SparkApplicationState `yaml:"stateTransitionHistory" json:"stateTransitionHistory"`
	ErrorMessage           string                           `yaml:"errorMessage" json:"errorMessage"`
}

type SparkApplicationState struct {
	State               string   `yaml:"state" json:"state"`
	CurrentStateSummary string   `yaml:"currentStateSummary" json:"currentStateSummary"`
	Message             string   `yaml:"message" json:"message"`
	LastTransitionTime  DateTime `yaml:"lastTransitionTime" json:"lastTransitionTime"`
}

type ResolvedSparkApplicationStatus struct {
	CurrentState string
	OutcomeState string
	Message      string
}

type SparkApplicationTaskStateTransition struct {
	State     string   `json:"state"`
	Message   string   `json:"message"`
	Timestamp DateTime `json:"timestamp"`
}

type SparkApplicationMetadata struct {
	Name        string            `yaml:"name" json:"name"`
	Namespace   string            `yaml:"namespace" json:"namespace"`
	Annotations map[string]string `yaml:"annotations,omitempty" json:"annotations,omitempty"`
}

type SparkApplicationManifestSpec struct {
	DeploymentMode         string                          `yaml:"deploymentMode" json:"deploymentMode"`
	PyFiles                string                          `yaml:"pyFiles" json:"pyFiles"`
	SparkConf              map[string]string               `yaml:"sparkConf" json:"sparkConf"`
	ApplicationTolerations SparkApplicationTolerations     `yaml:"applicationTolerations" json:"applicationTolerations"`
	DriverSpec             SparkApplicationRoleSpec        `yaml:"driverSpec" json:"driverSpec"`
	ExecutorSpec           SparkApplicationRoleSpec        `yaml:"executorSpec" json:"executorSpec"`
	RuntimeVersions        SparkApplicationRuntimeVersions `yaml:"runtimeVersions" json:"runtimeVersions"`
}

type SparkApplicationTolerations struct {
	InstanceConfig       SparkApplicationInstanceConfig `yaml:"instanceConfig" json:"instanceConfig"`
	ResourceRetainPolicy string                         `yaml:"resourceRetainPolicy" json:"resourceRetainPolicy"`
}

type SparkApplicationInstanceConfig struct {
	InitExecutors int `yaml:"initExecutors" json:"initExecutors"`
	MinExecutors  int `yaml:"minExecutors" json:"minExecutors"`
	MaxExecutors  int `yaml:"maxExecutors" json:"maxExecutors"`
}

type SparkApplicationRoleSpec struct {
	PodTemplateSpec SparkApplicationPodTemplateSpec `yaml:"podTemplateSpec" json:"podTemplateSpec"`
}

type SparkApplicationPodTemplateSpec struct {
	Spec SparkApplicationPodSpec `yaml:"spec" json:"spec"`
}

type SparkApplicationPodSpec struct {
	ServiceAccountName string                          `yaml:"serviceAccountName" json:"serviceAccountName"`
	Containers         []SparkApplicationContainerSpec `yaml:"containers" json:"containers"`
}

type SparkApplicationContainerSpec struct {
	Name      string                               `yaml:"name" json:"name"`
	Env       []SparkApplicationEnvVar             `yaml:"env" json:"env"`
	Resources SparkApplicationResourceRequirements `yaml:"resources" json:"resources"`
}

type SparkApplicationEnvVar struct {
	Name  string `yaml:"name" json:"name"`
	Value string `yaml:"value" json:"value"`
}

type SparkApplicationResourceRequirements struct {
	Requests SparkApplicationResourceList `yaml:"requests" json:"requests"`
	Limits   SparkApplicationResourceList `yaml:"limits" json:"limits"`
}

type SparkApplicationResourceList struct {
	CPU    string `yaml:"cpu" json:"cpu"`
	Memory string `yaml:"memory" json:"memory"`
}

type SparkApplicationRuntimeVersions struct {
	ScalaVersion string `yaml:"scalaVersion" json:"scalaVersion"`
	SparkVersion string `yaml:"sparkVersion" json:"sparkVersion"`
}

func LoadSparkApplicationTemplate() (*SparkApplicationManifest, error) {
	var manifest SparkApplicationManifest
	if err := yaml.Unmarshal(buildassets.SparkApplicationTemplates, &manifest); err != nil {
		return nil, fmt.Errorf("could not unmarshal spark application template: %w", err)
	}

	return &manifest, nil
}

func LoadRewriteDataFilesTemplate() (*SparkApplicationManifest, error) {
	return LoadSparkApplicationTemplate()
}

func (m *SparkApplicationManifest) SetPyFileName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("spark application pyFiles name is required")
	}

	current := strings.TrimSpace(m.Spec.PyFiles)
	if current == "" {
		m.Spec.PyFiles = name
		return nil
	}

	idx := strings.LastIndex(current, "/")
	if idx == -1 {
		m.Spec.PyFiles = name
		return nil
	}

	m.Spec.PyFiles = current[:idx+1] + name

	return nil
}

func (m *SparkApplicationManifest) DriverContainer() (*SparkApplicationContainerSpec, error) {
	containers := m.Spec.DriverSpec.PodTemplateSpec.Spec.Containers
	if len(containers) == 0 {
		return nil, fmt.Errorf("spark application template has no driver containers")
	}

	return &m.Spec.DriverSpec.PodTemplateSpec.Spec.Containers[0], nil
}

func (m *SparkApplicationManifest) SetAnnotation(name, value string) {
	if m.Metadata.Annotations == nil {
		m.Metadata.Annotations = make(map[string]string)
	}

	m.Metadata.Annotations[name] = value
}

func (m *SparkApplicationManifest) SetEnvValues(values map[string]string) error {
	driverContainer, err := m.DriverContainer()
	if err != nil {
		return err
	}

	for k, v := range values {
		driverContainer.SetEnvValue(k, v)
	}

	return nil
}

func (c *SparkApplicationContainerSpec) SetEnvValue(name, value string) {
	for i := range c.Env {
		if c.Env[i].Name == name {
			c.Env[i].Value = value
			return
		}
	}

	c.Env = append(c.Env, SparkApplicationEnvVar{Name: name, Value: value})
}

func (m *SparkApplicationManifest) ToCreateUnstructured() (*unstructured.Unstructured, error) {
	payload, err := json.Marshal(sparkApplicationCreateManifest{
		APIVersion: m.APIVersion,
		Kind:       m.Kind,
		Metadata:   m.Metadata,
		Spec:       m.Spec,
	})
	if err != nil {
		return nil, fmt.Errorf("could not marshal spark application manifest: %w", err)
	}

	obj := make(map[string]any)
	if err = json.Unmarshal(payload, &obj); err != nil {
		return nil, fmt.Errorf("could not convert spark application manifest to unstructured object: %w", err)
	}

	return &unstructured.Unstructured{Object: obj}, nil
}

func UnstructuredToSparkApplicationManifest(resource *unstructured.Unstructured) (*SparkApplicationManifest, error) {
	payload, err := json.Marshal(resource.Object)
	if err != nil {
		return nil, fmt.Errorf("could not marshal spark application resource: %w", err)
	}

	var manifest SparkApplicationManifest
	if err = json.Unmarshal(payload, &manifest); err != nil {
		return nil, fmt.Errorf("could not unmarshal spark application resource: %w", err)
	}

	return &manifest, nil
}

func (s SparkApplicationStatus) Resolve() ResolvedSparkApplicationStatus {
	currentState := firstNonEmpty(s.CurrentState.Summary(), s.ApplicationState.Summary())
	currentMessage := firstNonEmpty(s.CurrentState.Message, s.ErrorMessage)

	var latestMeaningful SparkApplicationState
	var latestFailure SparkApplicationState
	var latestSuccess SparkApplicationState
	var latestTerminal SparkApplicationState

	for _, transition := range s.sortedTransitions() {
		state := transition.Summary()
		if state == "" {
			continue
		}

		latestMeaningful = transition

		if isSparkApplicationFailureState(state) {
			latestFailure = transition
		}

		if isSparkApplicationSuccessState(state) {
			latestSuccess = transition
		}

		if isSparkApplicationTerminalState(state) && !isSparkApplicationTerminalWrapperState(state) {
			latestTerminal = transition
		}
	}

	outcomeState := currentState
	outcomeMessage := currentMessage

	switch {
	case isSparkApplicationTerminalWrapperState(currentState):
		if latestFailure.Summary() != "" {
			outcomeState = latestFailure.Summary()
			outcomeMessage = firstNonEmpty(latestFailure.Message, currentMessage)
		} else if latestSuccess.Summary() != "" {
			outcomeState = latestSuccess.Summary()
			outcomeMessage = firstNonEmpty(latestSuccess.Message, currentMessage)
		} else if latestTerminal.Summary() != "" {
			outcomeState = latestTerminal.Summary()
			outcomeMessage = firstNonEmpty(latestTerminal.Message, currentMessage)
		}
	case outcomeState == "":
		if latestTerminal.Summary() != "" {
			outcomeState = latestTerminal.Summary()
			outcomeMessage = firstNonEmpty(latestTerminal.Message, currentMessage)
		} else if latestMeaningful.Summary() != "" {
			outcomeState = latestMeaningful.Summary()
			outcomeMessage = firstNonEmpty(latestMeaningful.Message, currentMessage)
		}
	case outcomeMessage == "":
		switch outcomeState {
		case latestFailure.Summary():
			outcomeMessage = latestFailure.Message
		case latestSuccess.Summary():
			outcomeMessage = latestSuccess.Message
		case latestTerminal.Summary():
			outcomeMessage = latestTerminal.Message
		case latestMeaningful.Summary():
			outcomeMessage = latestMeaningful.Message
		}
	}

	return ResolvedSparkApplicationStatus{
		CurrentState: currentState,
		OutcomeState: firstNonEmpty(outcomeState, currentState),
		Message:      firstNonEmpty(outcomeMessage, currentMessage),
	}
}

func (s SparkApplicationState) Summary() string {
	return firstNonEmpty(s.CurrentStateSummary, s.State)
}

func (s SparkApplicationStatus) sortedTransitions() []SparkApplicationState {
	if len(s.StateTransitionHistory) == 0 {
		return nil
	}

	keys := make([]string, 0, len(s.StateTransitionHistory))
	for key := range s.StateTransitionHistory {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		left, leftErr := strconv.Atoi(keys[i])
		right, rightErr := strconv.Atoi(keys[j])

		switch {
		case leftErr == nil && rightErr == nil:
			return left < right
		case leftErr == nil:
			return true
		case rightErr == nil:
			return false
		default:
			return keys[i] < keys[j]
		}
	})

	transitions := make([]SparkApplicationState, 0, len(keys))
	for _, key := range keys {
		transitions = append(transitions, s.StateTransitionHistory[key])
	}

	return transitions
}

func (s ResolvedSparkApplicationStatus) State() string {
	return firstNonEmpty(s.OutcomeState, s.CurrentState)
}

func (s ResolvedSparkApplicationStatus) IsTerminal() bool {
	return isSparkApplicationTerminalState(s.CurrentState) || isSparkApplicationTerminalState(s.OutcomeState)
}

func (s ResolvedSparkApplicationStatus) IsSuccess() bool {
	return isSparkApplicationSuccessState(s.State())
}

func (s SparkApplicationStatus) TransitionResults() []SparkApplicationTaskStateTransition {
	transitions := s.sortedTransitions()
	if len(transitions) == 0 {
		return nil
	}

	results := make([]SparkApplicationTaskStateTransition, 0, len(transitions))
	for _, transition := range transitions {
		state := transition.Summary()
		if state == "" {
			continue
		}

		results = append(results, SparkApplicationTaskStateTransition{
			State:     state,
			Message:   transition.Message,
			Timestamp: transition.LastTransitionTime,
		})
	}

	return results
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func sanitizeK8sName(name string) string {
	name = strings.ToLower(name)

	var b strings.Builder
	b.Grow(len(name))
	lastDash := false

	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}

		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	result := strings.Trim(b.String(), "-")
	if result == "" {
		return "spark-application"
	}

	if len(result) > 63 {
		result = strings.Trim(result[:63], "-")
	}

	if result == "" {
		return "spark-application"
	}

	return result
}
