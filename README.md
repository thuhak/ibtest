## ib网络测试工具

用来测试ib网络内所有节点的通信性能，找到有问题的节点

- 需要当前能够ssh无密码登陆到所有节点
- 需要有sudo权限

用法:

```bash
./ibtest.py -q QUEUE ib_read_bw
```