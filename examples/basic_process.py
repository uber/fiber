from fiber import Process

def f(name):
    print('Hello', name)

if __name__ == "__main__":
    p = Process(target=f, args=('Fiber',))
    p.start()
    p.join()
    print('Done')
