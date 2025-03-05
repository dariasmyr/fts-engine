package models

type Meta struct {
	URI       string `json:"uri"`
	RequestID string `json:"request_id"`
	ID        string `json:"id"`
	DT        string `json:"dt"`
	Domain    string `json:"domain"`
	Stream    string `json:"stream"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type LogParams struct {
	Tags []string `json:"tags"`
}

type Event struct {
	Meta             Meta      `json:"meta"`
	ID               int64     `json:"id"`
	Type             string    `json:"type"`
	Namespace        int       `json:"namespace"`
	Title            string    `json:"title"`
	TitleURL         string    `json:"title_url"`
	Comment          string    `json:"comment"`
	Timestamp        int64     `json:"timestamp"`
	User             string    `json:"user"`
	Bot              bool      `json:"bot"`
	LogID            int       `json:"log_id"`
	LogType          string    `json:"log_type"`
	LogAction        string    `json:"log_action"`
	LogParams        LogParams `json:"log_params"`
	LogActionComment string    `json:"log_action_comment"`
	ServerURL        string    `json:"server_url"`
	ServerName       string    `json:"server_name"`
	ServerScriptPath string    `json:"server_script_path"`
	Wiki             string    `json:"wiki"`
	ParsedComment    string    `json:"parsedcomment"`
}
