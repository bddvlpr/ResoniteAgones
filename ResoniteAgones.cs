using Agones;
using FrooxEngine;
using Grpc.Core;
using ResoniteModLoader;

namespace ResoniteAgones;

public class ResoniteAgones : ResoniteMod
{
    internal const string VERSION = "0.0.1";

    private static readonly TimeSpan HealthInterval = TimeSpan.FromSeconds(3);

    public override string Name => "ResoniteAgones";

    public override string Author => "Nali <nali@birds.avali.network>";

    public override string Version => VERSION;

    private CancellationTokenSource _lifetime = new();
    private readonly AgonesSDK _agones = new();
    private readonly SemaphoreSlim _stateLock = new(1, 1);

    private bool _hasLoadedWorld;
    private GameServerState? _lastState;
    private int? _lastReportedPlayerCount;

    public override void OnEngineInit()
    {
        Engine.Current.RunPostInit(() =>
        {
            var worldManager = Engine.Current.WorldManager;
            worldManager.WorldAdded += OnWorldAdded;
            worldManager.WorldRemoved += OnWorldRemoved;

            Engine.Current.OnShutdownRequest += (_) => _lifetime.Cancel();

            foreach (var world in worldManager.Worlds)
            {
                SubscribeWorld(world);
                _hasLoadedWorld = true;
            }

            _ = HealthLoopAsync();
            _ = ReconcileStateAsync();
        });
    }

    private void OnWorldAdded(World world)
    {
        SubscribeWorld(world);
        _hasLoadedWorld = true;

        Msg($"World '{world.Name}' has been added ({GetWorldCount()} worlds).");

        _ = ReconcileStateAsync();
    }

    private void OnWorldRemoved(World world)
    {
        UnsubscribeWorld(world);

        Msg($"World '{world.Name}' has been removed ({GetWorldCount()} worlds remain).");

        _ = ReconcileStateAsync();
    }

    private void SubscribeWorld(World world)
    {
        UnsubscribeWorld(world);

        world.UserJoined += OnUserJoined;
        world.UserLeft += OnUserLeft;
    }

    private void UnsubscribeWorld(World world)
    {
        world.UserJoined -= OnUserJoined;
        world.UserLeft -= OnUserLeft;
    }

    private void OnUserJoined(User user)
    {
        if (user.IsLocalUser)
        {
            return;
        }

        Msg($"User '{user.UserName}' ({user.UserID}) joined world '{user.World.Name}'.");

        _ = ReconcileStateAsync();
    }

    private void OnUserLeft(User user)
    {
        if (user.IsLocalUser)
        {
            return;
        }

        Msg($"User '{user.UserName}' ({user.UserID}) left world '{user.World.Name}'.");

        _ = ReconcileStateAsync();
    }

    private async Task HealthLoopAsync()
    {
        while (!_lifetime.IsCancellationRequested)
        {
            try
            {
                var status = await _agones.HealthAsync();

                if (_lifetime.IsCancellationRequested)
                {
                    break;
                }

                if (status.StatusCode == StatusCode.OK)
                {
                    _ = ReconcileStateAsync();
                }
                else
                {
                    Error($"Agones Health request failed. {status.StatusCode} {status.Detail}");
                }
            }
            catch (Exception exception)
            {
                if (_lifetime.IsCancellationRequested)
                {
                    break;
                }

                Warn($"Agones Health request failed. {exception}");
            }

            try
            {
                await Task.Delay(HealthInterval, _lifetime.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task ReconcileStateAsync()
    {
        await Task.Yield();

        await _stateLock.WaitAsync();

        try
        {
            var playerCount = Engine.Current.WorldManager.Worlds
                .SelectMany(world => world.AllUsers)
                .Count(user => !user.IsLocalUser);

            var desiredState = GetWorldCount() == 0
                ? _hasLoadedWorld ? GameServerState.Shutdown : (GameServerState?)null
                : playerCount > 0 ? GameServerState.Allocated : GameServerState.Ready;

            if (_lastState == GameServerState.Shutdown)
            {
                return;
            }

            if (desiredState is not null && desiredState != _lastState)
            {
                try
                {
                    var status = desiredState.Value switch
                    {
                        GameServerState.Ready => await _agones.ReadyAsync(),
                        GameServerState.Allocated => await _agones.AllocateAsync(),
                        GameServerState.Shutdown => await _agones.ShutDownAsync(),
                        _ => throw new ArgumentOutOfRangeException(),
                    };

                    if (status.StatusCode == StatusCode.OK)
                    {
                        _lastState = desiredState;

                        if (desiredState == GameServerState.Shutdown)
                        {
                            _lifetime.Cancel();
                        }

                        Msg($"Agones state set to {desiredState} ({GetWorldCount()} worlds, {playerCount} players).");
                    }
                    else
                    {
                        Error($"Agones {desiredState} request failed. {status.StatusCode} {status.Detail}");
                    }
                }
                catch (Exception exception)
                {
                    Warn($"Agones {desiredState} request failed. {exception}");
                }
            }

            if (_lastState != GameServerState.Shutdown && _lastReportedPlayerCount != playerCount)
            {
                try
                {
                    await _agones.Beta().SetCounterCountAsync("players", playerCount);
                    _lastReportedPlayerCount = playerCount;
                    Msg($"Agones players counter set to {playerCount}.");
                }
                catch (Exception exception)
                {
                    Warn($"Agones players counter update failed. {exception}");
                }
            }
        }
        catch (Exception exception)
        {
            Warn($"Agones state reconciliation failed. {exception}");
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private static int GetWorldCount()
    {
        return Engine.Current.WorldManager.Worlds
            .Count(world => world is not null);
    }

    private enum GameServerState
    {
        Ready,
        Allocated,
        Shutdown,
    }
}
