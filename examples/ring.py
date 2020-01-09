# This is a demo of how to use Fiber to start a multiple processes / jobs that
# forms a ring structure.
#
# The key thing to do is the initialization function `pytorch_ring_init` and
# worker function `pytorch_run_sgd`.
#
# This demo is adapted from https://pytorch.org/tutorials/intermediate/dist_tuto.html

import math
import random
import time

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torchvision.datasets as datasets
import torchvision.transforms as transforms

import fiber
from fiber.experimental import Ring


class Partition:
    """ Dataset partitioning helper """

    def __init__(self, data, index):
        self.data = data
        self.index = index

    def __len__(self):
        return len(self.index)

    def __getitem__(self, index):
        data_idx = self.index[index]
        return self.data[data_idx]


class DataPartitioner():
    """ Helper class to create data partitions """
    def __init__(self, data, sizes=[0.7, 0.2, 0.1], seed=1234):
        self.data = data
        self.partitions = []
        rng = random
        rng.seed(seed)
        data_len = len(data)
        indexes = [x for x in range(0, data_len)]
        rng.shuffle(indexes)

        for frac in sizes:
            part_len = int(frac * data_len)
            self.partitions.append(indexes[0:part_len])
            indexes = indexes[part_len:]

    def use(self, partition):
        return Partition(self.data, self.partitions[partition])


def partition_dataset():
    """ Partitioning MNIST """
    dataset = datasets.MNIST(
        "./data",
        train=True,
        download=True,
        transform=transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        ),
    )
    size = dist.get_world_size()
    bsz = int(128 / float(size))
    partition_sizes = [1.0 / size for _ in range(size)]
    partition = DataPartitioner(dataset, partition_sizes)
    partition = partition.use(dist.get_rank())
    train_set = torch.utils.data.DataLoader(
        partition, batch_size=bsz, shuffle=True
    )
    return train_set, bsz


def average_gradients(model):
    """ Gradient averaging. """
    size = float(dist.get_world_size())
    for param in model.parameters():
        dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
        param.grad.data /= size


class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 20, 5, 1)
        self.conv2 = nn.Conv2d(20, 50, 5, 1)
        self.fc1 = nn.Linear(4 * 4 * 50, 500)
        self.fc2 = nn.Linear(500, 10)

    def forward(self, x):
        x = F.relu(self.conv1(x))
        x = F.max_pool2d(x, 2, 2)
        x = F.relu(self.conv2(x))
        x = F.max_pool2d(x, 2, 2)
        x = x.view(-1, 4 * 4 * 50)
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)


@fiber.meta(gpu=1)
def pytorch_run_sgd(rank, size):
    """ Distributed Synchronous SGD Example """
    torch.manual_seed(1234)
    train_set, bsz = partition_dataset()
    model = Net()

    if torch.cuda.is_available():
        model.to('cuda')

    optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.5)

    num_batches = math.ceil(len(train_set.dataset) / float(bsz))
    for epoch in range(10):
        epoch_loss = 0.0
        for data, target in train_set:
            optimizer.zero_grad()
            if torch.cuda.is_available():
                data = data.to('cuda')
                target = target.to('cuda')
            output = model(data)
            loss = F.nll_loss(output, target)
            epoch_loss += loss.item()
            loss.backward()
            average_gradients(model)
            optimizer.step()

        print("Rank ", dist.get_rank(), ", epoch ", epoch, ": ",
              epoch_loss / num_batches,)


def pytorch_ring_init(ring):
    """ Import necessary modules and setup the ring """
    import os
    from fiber.backend import get_backend
    import torch.distributed as dist

    backend = get_backend()
    rank = ring.rank
    master = ring.members[0]
    print("pytorch ring init, rank", rank)

    if rank != 0:
        wait = 0.1
        while master.connected is False:
            print("ring.memebers[0].connected != True, wait", wait)
            time.sleep(wait)
            wait = wait * 2
            master = ring.members[0]

    _, _, ifce = backend.get_listen_addr()

    os.environ["MASTER_ADDR"] = master.ip
    os.environ["MASTER_PORT"] = str(master.port)
    os.environ["GLOO_SOCKET_IFNAME"] = ifce

    print(
        ring.size,
        ring.rank,
        ifce,
        os.environ["MASTER_ADDR"],
        os.environ["MASTER_PORT"],
    )
    dist.init_process_group("gloo", rank=ring.rank, world_size=ring.size)


def main():
    #fiber.init(image="fiber-pytorch:latest")
    ring = Ring(2, pytorch_run_sgd, pytorch_ring_init)
    ring.run()


if __name__ == "__main__":
    main()
