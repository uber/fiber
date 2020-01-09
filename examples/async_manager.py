import gym
import time
from functools import partial
from fiber.managers import AsyncManager, SyncManager

NUM_ENVS = 4
STEPS = 5000

class EnvSyncManager(SyncManager):
    pass

class EnvAsyncManager(AsyncManager):
    pass

def sync_manager():
    EnvManager = EnvSyncManager

    create_cart_pole = partial(gym.make, "CartPole-v1")
    local_env = create_cart_pole()
    EnvManager.register("CartPoleEnv", create_cart_pole)

    managers = [EnvManager() for i in range(NUM_ENVS)]
    [manager.start() for manager in managers]

    envs = [manager.CartPoleEnv() for manager in managers]
    [env.reset() for env in envs]

    for i in range(STEPS):
        actions = [local_env.action_space.sample() for _ in range(len(envs))]
        results = [env.step(action) for env, action in zip(envs, actions)]
        for j in range(len(results)):
            observation, reward, done, info = results[j]
            if done:
                envs[j].reset()
        if len(envs) == 0:
            break

def async_manager():
    EnvManager = EnvAsyncManager
    create_cart_pole = partial(gym.make, "CartPole-v1")
    local_env = create_cart_pole()
    EnvManager.register("CartPoleEnv", create_cart_pole)

    managers = [EnvManager() for i in range(NUM_ENVS)]
    [manager.start() for manager in managers]

    envs = [manager.CartPoleEnv() for manager in managers]
    [env.reset() for env in envs]

    for i in range(STEPS):
        actions = [local_env.action_space.sample() for _ in range(len(envs))]
        handles = [env.step(action) for env, action in zip(envs, actions)]
        results = [handle.get() for handle in handles]
        for j in range(len(results)):
            observation, reward, done, info = results[j]
            if done:
                envs[j].reset()
        if len(envs) == 0:
            break

def main():
    start = time.time()
    sync_manager()
    print("Sync manager took {}s".format(time.time() - start))

    start = time.time()
    async_manager()
    print("Async manager took {}s".format(time.time() - start))

if __name__ == '__main__':
    main()
