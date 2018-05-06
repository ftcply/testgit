import subprocess

class _AttributeString(str):
    @property
    def stdout(self):
        return str(self)


def cmd(system_cmd, strict=False):
    pipe = subprocess.Popen(system_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True, stdin=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    return_code = pipe.returncode
    out = _AttributeString(stdout.strip() if stdout else "")
    out.fail = False if return_code == 0 else True
    out.success = True if return_code == 0 else False
    out.error = stderr if stderr else stdout.strip()
    if strict and out.fail:
        raise Exception(out.error)
    return out


x = cmd('ls -l | head -5')
print(x)
