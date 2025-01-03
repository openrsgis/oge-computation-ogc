package whu.edu.cn.oge.extension.docker

object DockerExecutor {
  def getExecutor(): Docker = {
    if (SwarmDocker.available()) {
      SwarmDocker
    } else if (K8SDocker.available()) {
      K8SDocker
    } else {
      throw new IllegalArgumentException("Docker 配置错误")
    }
  }
}
