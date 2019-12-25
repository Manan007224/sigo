package main


// Job ..
type Job struct {
	Jid              string
	Name             string
	Args             map[string]interface{}
	Queue			 string
}

// NewJob ..
func NewJob(jid, name, queue string, args map[string]interface{}) *Job {
	return &Job{
		Jid:  	jid,
		Name: 	name,
		Args: 	args,
		Queue:	queue,		  
	}
}
