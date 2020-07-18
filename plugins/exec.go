package plugins

import (
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/kardianos/osext"
	"github.com/runner-mei/errors"
	"github.com/runner-mei/goutils/as"
	"github.com/runner-mei/kinglink/core"
	"github.com/runner-mei/log"
)

var (
	binDir, _ = osext.ExecutableFolder()
)

func Exec(ctx *core.Context, job *core.Job) error {
	fields, err := job.Payload.Fields()
	if err != nil {
		return errors.Wrap(err, "参数格式不正确")
	}

	execute := as.StringWithDefault(fields["execute"], "")
	if execute == "" {
		return errors.NewArgumentMissing("execute")
	}

	args := as.StringsWithDefault(fields["arguments"], nil)

	var cmd *exec.Cmd
	if len(args) == 0 {
		cmd = exec.CommandContext(ctx, execute)
	} else {
		cmd = exec.CommandContext(ctx, execute, args...)
	}

	currentDir := as.StringWithDefault(fields["arguments"], "")
	if currentDir != "" {
		cmd.Dir = strings.Replace(currentDir, "${bindir}", binDir, -1)
		cmd.Dir = strings.Replace(cmd.Dir, "${cmddir}", filepath.Dir(execute), -1)
	}

	bs, err := cmd.CombinedOutput()
	if err != nil {
		if len(bs) > 0 {
			err = errors.WrapWithSuffix(err, string(bs))
		}
		return errors.Wrap(err, "执行外部程序失败")
	}

	log.For(ctx).Debug("执行外部程序成功", log.ByteString("output", bs))
	return nil
}

func init() {
	core.DefaultServeMux.Handle("exec", core.HandlerFunc(Exec))
}
