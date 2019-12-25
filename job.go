package main


// Job ..
type Job struct {
	Jid              string
	Name             string
	Args             []interface{}
	Queue			 string
}

// NewJob ..
func NewJob(jid, name, queue string, args []interface{}) *Job {
	return &Job{
		Jid:  	jid,
		Name: 	name,
		Args: 	args,
		Queue:	queue,		  
	}
}
