import fiber
from fiber.managers import BaseManager


# Source: https://stackoverflow.com/questions/9436757/how-does-multiprocessing-manager-work-in-python # noqa E501
def f(ns, ls, di):
    ns.x += 1
    ns.y[0] += 1
    ns_z = ns.z
    ns_z[0] += 1
    ns.z = ns_z

    ls[0] += 1
    ls[1][0] += 1  # unmanaged, not assigned back
    ls_2 = ls[2]   # unmanaged...
    ls_2[0] += 1
    ls[2] = ls_2   # ... but assigned back
    ls[3][0] += 1  # managed, direct manipulation

    di[0] += 1
    di[1][0] += 1  # unmanaged, not assigned back
    di_2 = di[2]   # unmanaged...
    di_2[0] += 1
    di[2] = di_2   # ... but assigned back
    di[3][0] += 1  # managed, direct manipulation


def run_manager_standard():
    manager = fiber.Manager()
    ns = manager.Namespace()
    ns.x = 1
    ns.y = [1]
    ns.z = [1]
    ls = manager.list([1, [1], [1], manager.list([1])])
    di = manager.dict({0: 1, 1: [1], 2: [1], 3: manager.list([1])})
    print('before', ns, ls, ls[2], di, di[2], sep='\n')
    p = fiber.Process(target=f, args=(ns, ls, di))
    p.start()
    p.join()
    print('after', ns, ls, ls[2], di, di[2], sep='\n')


class MathsClass:
    def add(self, x, y):
        return x + y

    def mul(self, x, y):
        return x * y


class MyManager(BaseManager):
    pass


def run_manager_customized():
    MyManager.register('Maths', MathsClass)
    with MyManager() as manager:
        maths = manager.Maths()
        print(maths.add(4, 3))
        print(maths.mul(7, 8))


def main():
    run_manager_standard()
    run_manager_customized()


if __name__ == '__main__':
    main()
