#!/usr/bin/env python
#coding: utf8
#author: Lubin

import os
import time
import json

from multiprocessing import Process, Manager
from string import Template

IRQ_LIST = Template("/proc/irq/$irqnum/smp_affinity_list")
ROUTE = "/proc/net/route"

BOND_MEM = Template("/sys/class/net/$bond/bonding/slaves")
BOND_CARD = Template("/proc/net/bonding/$bond")

NETCARD_SPEED = Template("/sys/class/net/$netcard/speed")
NETCARD_QUEUE = Template("/sys/class/net/$netcard/queues")

CPU_NUM = "/sys/devices/system/cpu/online"
CPU_STAT = "/proc/stat"
CPU_PHYSICAL_ID = \
    Template("/sys/devices/system/cpu/cpu$cpunum/topology/physical_package_id")
CPU_CORE_ID = \
    Template("/sys/devices/system/cpu/cpu$cpunum/topology/core_id")

DEFAULT_SOCKET = 1


class UnknownNetCard(Exception):
    pass


class UnknownMode(Exception):
    pass


class Cpu(object):

    @classmethod
    def CPU_INFO(cls):
        ''' 获取cpu信息'''
        cpu_num = cls.CPU_NUM()
        cpuinfo = {}
        for i in range(cpu_num + 1):
            socket_id = cls.CPU_PHYSICAL_ID(i)
            core_id = cls.CPU_CORE_ID(i)
            if socket_id not in cpuinfo:
                cpuinfo[socket_id] = {}
            if core_id not in cpuinfo[socket_id]:
                cpuinfo[socket_id][core_id] = []
            cpuinfo[socket_id][core_id].append(i)
        return cpuinfo

    @classmethod
    def CPU_NUM(cls):
        ''' 获取cpu数量 '''
        with open(CPU_NUM) as f:
            return int(f.readline().strip().split('-')[1])

    @classmethod
    def CPU_PHYSICAL_ID(cls, cpunum):
        ''' 获取cpu物理id '''
        with open(CPU_PHYSICAL_ID.substitute(cpunum=cpunum)) as f:
            return int(f.readline().strip())

    @classmethod
    def CPU_CORE_ID(cls, cpunum):
        ''' 获取cpu core id '''
        with open(CPU_CORE_ID.substitute(cpunum=cpunum)) as f:
            return int(f.readline().strip())

    @classmethod
    def CPU_STAT(cls, cpunum):
        ''' 获取cpu状态 '''
        with open(CPU_STAT) as f:
            for line in f:
                l = line.strip().split()
                if l[0] == cpunum:
                    return [float(x) for x in l[1:]]

    @classmethod
    def GetCpuLoad(cls, cpunum):
        ''' 获取cpu负载 '''
        user, nice, system, idle, iowait, \
            irq, softrig, steal, _drop = cls.CPU_STAT(cpunum)

        old_idle = idle + iowait
        old_busy = user + nice + system + irq + softrig + steal
        old_total = old_idle + old_busy

        time.sleep(1)
        user, nice, system, idle, iowait, \
            irq, softrig, steal, _drop = cls.CPU_STAT(cpunum)
        new_idle = idle + iowait
        new_busy = user + nice + system + irq + softrig + steal
        new_total = new_idle + new_busy

        cpu_total = new_total - old_total
        cpu_idle = new_idle - old_idle

        return (cpu_total - cpu_idle) / cpu_total * 100

    @classmethod
    def GetAllCpuLoad(cls):
        ''' 获取所有cpu负载 '''
        cpu_num = cls.CPU_NUM()
        process = []
        d = Manager().dict()

        def _deco(func):
            def _f(*args, **kwargs):
                load = func("cpu%s" % str(args[0]))
                args[1][args[0]] = load
            return _f

        f = _deco(cls.GetCpuLoad)
        for i in range(cpu_num + 1):
            p = Process(target=f, args=(i, d))
            process.append(p)
            p.start()

        for p in process:
            p.join()

        return dict(d)


class Mode(object):

    def __init__(self, socket):
        self.socket = socket

    def _set_affinity(self, irqnum, cpuset):
        ''' 绑定中断号到cpu '''
        s = ''
        if isinstance(cpuset, str):
            s = cpuset
        elif isinstance(cpuset, list):
            s = ','.join(str(x) for x in cpuset)
        else:
            raise ValueError

        with open(IRQ_LIST.substitute(irqnum=irqnum), 'w') as f:
            f.write(s)

    def SetAffinity(self, irqDict):
        ''' 子类应实现 '''
        raise NotImplementedError


class ModeOne(Mode):
    '''
    折中模式，将网卡中断分配到一个插槽的每个物理Core上
    tx_num ％ core_id
    '''
    def __init__(self, socket):
        super(ModeOne, self).__init__(socket)

    def SetAffinity(self, irqDict):
        coreList = Cpu.CPU_INFO()[self.socket]
        coreNum = len(coreList)
        i = 0

        for netcard in irqDict:
            for irqinfo in irqDict[netcard]:
                cpuset = coreList[i]
                self._set_affinity(irqinfo[1], cpuset)
                i += 1
                if i == coreNum:
                    i = 0


class ModeTwo(Mode):
    '''
    集中模式，每个网卡分配一个独立的Core，丢包率最低
    netcard_num % core_id
    如果是万兆网卡 就按tx_num % core_id
    '''
    def __init__(self, socket):
        super(ModeTwo, self).__init__(socket)

    def SetAffinity(self, irqDict):
        coreList = Cpu.CPU_INFO()[self.socket]
        coreNum = len(coreList)
        j = 0

        for netcard in irqDict:
            if NetCard.IsTenGigabit(netcard):
                i = 0
                for irqinfo in irqDict[netcard]:
                    cpuset = coreList[i % coreNum]
                    self._set_affinity(irqinfo[1], cpuset)
                    i += 1
            else:
                cpuset = coreList[j]
                for irqinfo in irqDict[netcard]:
                    self._set_affinity(irqinfo[1], cpuset)
                j += 1


class ModeThree(Mode):
    '''
    离散模式，将网卡中断分配到每个物理Core上
    tx_num % (core_id * socket)
    '''
    def __init__(self, socket):
        super(ModeThree, self).__init__(socket)

    def SetAffinity(self, irqDict):
        allcpu = []
        cpuinfo = Cpu.CPU_INFO()
        for socket in cpuinfo:
            for coreid in cpuinfo[socket]:
                allcpu.append(cpuinfo[socket][coreid])

        i = 0
        for netcard in irqDict:
            for irqinfo in irqDict[netcard]:
                self._set_affinity(irqinfo[1], allcpu[i])
                i += 1
                if i > len(allcpu) - 1:
                    i = 0


#######################################################################


class NetCard(object):

    def __init__(self, netcard):
        self.netcard = netcard

    def _get_interrupts_num(self, netcard):
        ''' 获取网卡中断号列表 '''
        Tx = []
        STx = []
        with open('/proc/interrupts', 'r') as f:
            for line in f:
                for s in line.split():
                    if s.find(netcard + '-') == 0:
                        i_s = line.strip().split()
                        Tx.append([i_s[len(i_s) - 1], i_s[0].split(':')[0]])
                    elif s.find(netcard) == 0:
                        i_s = line.strip().split()
                        STx.append([i_s[len(i_s) - 1], i_s[0].split(':')[0]])
        if len(Tx) > 0:
            return Tx
        return STx

    def _get_smp_affinity(self, irqnum):
        ''' 获取网卡和cpu affinity 列表 '''
        with open(IRQ_LIST.substitute(irqnum=irqnum)) as f:
            return f.readline().strip().split(',')

    @classmethod
    def IsBond(cls, netcard):
        return os.path.isfile(BOND_CARD.substitute(bond=netcard))

    @classmethod
    def IsTenGigabit(cls, netcard):
        with open(NETCARD_SPEED.substitute(netcard=netcard)) as f:
            return int(f.readline().strip()) >= 10000

    def PrintAffinityInfo(self):
        ''' 打印irq 绑定信息 '''
        irqDict = self.GenInterruptsDict()
        for netcard in irqDict:
            print netcard, ": "
            for irqinfo in irqDict[netcard]:
                print "{q}\tIRQ: {i}   Affinity: {a}".format(
                    q=irqinfo[0], i=irqinfo[1],
                    a=self._get_smp_affinity(irqinfo[1])
                )

    def SetSmpAffinity(self, irqDict, mode):
        ''' 设置网卡和cpu affinity '''
        mode.SetAffinity(irqDict)

    def GenInterruptsDict(self):
        raise NotImplementedError


class BondCard(NetCard):
    ''' Bonding 网卡 '''
    def __init__(self, netcard):
        super(BondCard, self).__init__(netcard)
        self.bonds = self._get_bond_member()

    def _get_bond_member(self):
        with open(BOND_MEM.substitute(bond=self.netcard)) as f:
            return f.readline().strip().split()

    def GenInterruptsDict(self):
        irqDict = {}
        for netcard in self._get_bond_member():
            irqDict[netcard] = self._get_interrupts_num(netcard)
        return irqDict


class TenGigabitCard(NetCard):
    ''' 万兆网卡 '''
    def __init__(self, netcard):
        super(TenGigabitCard, self).__init__(netcard)

    def GenInterruptsDict(self):
        return {self.netcard: self._get_interrupts_num(self.netcard)}


class NetCardFactory(object):
    ''' 网卡工厂 '''
    @classmethod
    def CreateNetCard(cls, netcard):
        if NetCard.IsBond(netcard):
            return BondCard(netcard)

        if NetCard.IsTenGigabit(netcard):
            return TenGigabitCard(netcard)

        raise UnknownNetCard


if __name__ == '__main__':

    def get_default_netcard():
        '''
        获取默认网关出口网卡
        '''
        with open(ROUTE) as f:
            for line in f:
                s = line.split()
                try:
                    if int(s[1]) == 0:
                        return s[0]
                except ValueError:
                    pass

    def Main(netcard, mode, socket):
        N = NetCardFactory.CreateNetCard(netcard)
        irqDict = N.GenInterruptsDict()
        if mode == 1:
            m = ModeOne(socket)
        elif mode == 2:
            m = ModeTwo(socket)
        elif mode == 3:
            m = ModeThree(socket)
        N.SetSmpAffinity(irqDict, m)
        N.PrintAffinityInfo()

    from optparse import OptionParser

    option = OptionParser()
    option.add_option(
        '-m', '--mode', dest='mode',
        help='Set Netcard CPU Affinity Mode')
    option.add_option(
        '-i', '--cpu_info', dest='list_cpu', action='store_true',
        default=False, help='List Cpu Info')
    option.add_option(
        '-d', '--default_network', dest='default_network', action='store_true',
        default=False, help='Look Default Network')
    option.add_option(
        '-n', '--netcard', dest='netcard', action='store',
        help='Bind Netcard')
    option.add_option(
        '-s', '--socket', dest='socket', action='store',
        help='Default CPU Socket')

    ops, args = option.parse_args()

    if ops.socket:
        DEFAULT_SOCKET = ops.socket

    if ops.list_cpu:
        print json.dumps(Cpu.CPU_INFO(), indent=4)
        os._exit(0)

    if ops.default_network:
        print "Defalut Netcard: ", get_default_netcard()

    if ops.mode:
        netcard = get_default_netcard()
        if ops.netcard:
            netcard = ops.netcard
        Main(netcard, int(ops.mode), int(DEFAULT_SOCKET))
