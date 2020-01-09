from fiber import Process, SimpleQueue

def foo(q, a, b):
    q.put(a + b)

if __name__ == '__main__':
    q = SimpleQueue()
    p = Process(target=foo, args=(q, 42, 21))
    p.start()
    print(q.get())
    p.join()
