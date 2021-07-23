# taskmanager [![Go](https://github.com/plexsysio/taskmanager/workflows/Go/badge.svg)](https://github.com/plexsysio/taskmanager/actions) [![Go Reference](https://pkg.go.dev/badge/github.com/plexsysio/taskmanager.svg)](https://pkg.go.dev/github.com/plexsysio/taskmanager) [![Coverage Status](https://coveralls.io/repos/github/plexsysio/taskmanager/badge.svg?branch=main)](https://coveralls.io/github/plexsysio/taskmanager?branch=main)

Async task manager. Tasks can easily be customized and executed asynchronously on
the next available worker.

The manager keeps workers ready to multiplex tasks. The maximum no. of workers can
be configured.

This package was mainly created to abstract all async functionality from the app. It
provides a consistent context interface to manage routine lifecycle from a single
place.

## Install
`taskmanager` works like a regular Go module:

```
> go get github.com/plexsysio/taskmanager
```

## Usage
```
import "github.com/plexsysio/taskmanager"

type exampleTask struct {}

func (e *exampleTask) Name() string {
   return "exampleTask"
}

func (e *exampleTask) Execute(ctx context.Context) error {
   for {
      select {
      case <-ctx.Done():
         // taskmanager stopped
         return nil
      default:
        // Do work. For long running tasks use ctx or move to next iteration
      }
   }
}

func main() {

   tm := taskmanager.New(1, 100, time.Second*15)
   t := &exampleTask{}

   sched, err := tm.Go(t)
   if err != nil {
      fmt.Println(err)
      return
   }

   // Task scheduled
   <-sched
   
```
Closures can also be scheduled
```
   fSched, err := tm.GoFunc(func(ctx context.Context) error {
      for {
         select {
         case <-ctx.Done():
            //taskmanager stopped
            return nil
         default:
            // Do work
         }
      }
   })
   if err != nil {
      fmt.Println(err)
      return
   }

   // Stop will wait for all routines to stop. Context can be passed here to
   // ensure timeout in Stop
   ctx, _ := context.WithTimeout(time.Second)
   err = tm.Stop(ctx)
   if err != nil {
      fmt.Printf("failed stopping %s\n", err.Error())
   }
}
```
