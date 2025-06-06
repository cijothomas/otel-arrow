"""
Module: lifecycle_component

This module defines the abstract base class `LifecycleComponent`, which serves as a blueprint for components
managed by the orchestrator. It provides hooks for various lifecycle phases such as configuration,
deployment, starting, stopping, and destruction, allowing for custom behavior during these phases.

Components derived from `LifecycleComponent` must implement the lifecycle methods (`configure`, `deploy`,
`start`, `stop`, and `destroy`) and can register hooks to be executed at specific points in the lifecycle.

Classes:
    LifecyclePhase: An enumeration of the different phases in the lifecycle of a component.
    LifecycleComponent: An abstract base class that defines the structure for components with lifecycle hooks.
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Any

from .runtime import ComponentRuntime
from ..context.base import BaseContext, ExecutionStatus
from ..context.test_contexts import TestStepContext, TestExecutionContext


@dataclass
class LifecycleHookContext(BaseContext):
    """
    Holds state for a test hook execution.
    """

    parent_ctx: Optional["TestStepContext"] = None
    phase: Optional["HookableLifecyclePhase"] = None

    def get_step_component(self) -> Optional["LifecycleComponent"]:
        """Fetches the component instance on which this hook is firing.

        Returns: the component instance or none.
        """
        if self.parent_ctx is None:
            raise RuntimeError(
                "LifecycleHookContext.parent_ctx must be set to access the step component."
            )
        return self.parent_ctx.step.component


class LifecyclePhase(Enum):
    """
    Enum representing the various primary phases in the lifecycle of a component.

    These phases help manage the orchestration of components during test execution.

    Phases include:
        - CONFIGURE        (call configuration strategies to e.g. prepare manifests for deployment)
        - DEPLOY           (call a deployment strategy to e.g. deploy / start a process/container)
        - START            (call an execution strategy to e.g. start sending load)
        - STOP             (call an execution strategy to e.g. stop sending load)
        - DESTROY          (call a deployment strategy to e.g. stop a process/container)
        - START_MONITORING (call a monitoring strategy to e.g. monitor a process / container)
        - STOP_MONITORING  (call a monitoring strategy to e.g. stop monitoring a process / container)
    """

    CONFIGURE = "configure"
    DEPLOY = "deploy"
    START = "start"
    STOP = "stop"
    DESTROY = "destroy"
    START_MONITORING = "start_monitoring"
    STOP_MONITORING = "stop_monitoring"


class HookableLifecyclePhase(Enum):
    """
    Enum representing the various phases in the lifecycle of a component which support hooks.

    These phases correspond to different stages of the component's lifecycle, where hooks can be registered
    and executed to perform actions before or after a phase is executed. These phases help manage the
    orchestration of components during test execution.

    Phases include:
        - PRE_CONFIGURE, POST_CONFIGURE
        - PRE_DEPLOY, POST_DEPLOY
        - PRE_START, POST_START
        - PRE_STOP, POST_STOP
        - PRE_DESTROY, POST_DESTROY
        - PRE_START_MONITORING, POST_START_MONITORING
        - PRE_STOP_MONITORING, POST_STOP_MONITORING
    """

    PRE_CONFIGURE = "pre_configure"
    POST_CONFIGURE = "post_configure"
    PRE_DEPLOY = "pre_deploy"
    POST_DEPLOY = "post_deploy"
    PRE_START = "pre_start"
    POST_START = "post_start"
    PRE_STOP = "pre_stop"
    POST_STOP = "post_stop"
    PRE_DESTROY = "pre_destroy"
    POST_DESTROY = "post_destroy"
    PRE_START_MONITORING = "pre_start_monitoring"
    POST_START_MONITORING = "post_start_monitoring"
    PRE_STOP_MONITORING = "pre_stop_monitoring"
    POST_STOP_MONITORING = "post_stop_monitoring"


class LifecycleComponent(ABC):
    """
    Abstract base class for components within a load generation test orchestrator.

    This class provides a mechanism for registering and executing hooks at various lifecycle phases, allowing
    subclasses to define specific behaviors during phases such as configuration, deployment, starting, stopping,
    and destruction. Subclasses are required to implement the lifecycle methods (`configure`, `deploy`, `start`,
    `stop`, and `destroy`).

    Components can register hooks that will be executed during specific lifecycle phases. Hooks are callable
    functions that are executed when a particular phase occurs, enabling custom actions at various points in the
    lifecycle.

    Attributes:
        _hooks (Dict[LifecyclePhase, List[Callable]]): A registry of hooks for each lifecycle phase,
                                                       where the key is the phase and the value is a list of
                                                       callable functions to execute during that phase.

    Methods:
        add_hook(phase, hook): Registers a hook function to be executed during a specified lifecycle phase.
        _run_hooks(phase): Executes all hooks that have been registered for a specified lifecycle phase.
        configure(): Abstract method to be implemented by subclasses for configuring the component.
        deploy(): Abstract method to be implemented by subclasses for deploying the component (e.g. spawn process, start container).
        start(): Abstract method to be implemented by subclasses for starting the component's execution behavior (e.g. send load).
        stop(): Abstract method to be implemented by subclasses for stopping the component's execution behavior (e.g. stop load).
        destroy(): Abstract method to be implemented by subclasses for destroying the component (e.g. kill process, stop/remove container).
        start_monitoring(): Abstract method to be implemented by subclasses to start monitoring the component.
        stop_monitoring(): Abstract method to be implemented by subclasses to stop monitoring the component.
        collect_monitoring_data(): Abstract method to be implemented by subclasses to collect monitoring data for the component.
    """

    def __init__(self):
        """
        Initializes the LifecycleComponent instance by setting up an empty hook registry.

        The hook registry maps lifecycle phases to lists of hook functions (callables). Hooks
        can be added to different phases, and when those phases are triggered, the corresponding hooks will
        be executed.
        """
        self._hooks: Dict[
            HookableLifecyclePhase, List[Callable[[LifecycleHookContext], Any]]
        ] = defaultdict(list)

        self.runtime: ComponentRuntime = ComponentRuntime()

    def get_or_create_runtime(self, namespace: str, factory: Callable[[], Any]) -> Any:
        """Get an existing runtime data structure or initialize a new one.

        Args:
            namespace: The namespace to get/create data for.
            factory: The initialization method if no namespace data exists.
        """
        return self.runtime.get_or_create(namespace, factory)

    def set_runtime_data(self, namespace: str, data: Any):
        """Set the data value on the component's runtime with the specified namespace.

        Args:
            namespace: The namespace to set the data value on.
            data: The data to set.
        """
        self.runtime.set(namespace, data)

    def add_hook(
        self, phase: HookableLifecyclePhase, hook: Callable[[LifecycleHookContext], Any]
    ):
        """
        Registers a hook to be executed during a specific lifecycle phase.

        Hooks allow you to define custom behavior during various lifecycle phases, such as configuring
        the component, deploying it, starting or stopping it, and more. Each hook is a callable function.

        Args:
            phase (LifecyclePhase): The lifecycle phase during which the hook should be executed.
                                     Example phases are "pre_configure", "post_configure", "pre_deploy", etc.
            hook (Callable): A function to be executed during the specified lifecycle phase.

        Example:
            component.add_hook(LifecyclePhase.PRE_DEPLOY, lambda: print("Preparing deployment..."))
        """
        self._hooks[phase].append(hook)

    def _run_hooks(self, phase: HookableLifecyclePhase, ctx: TestStepContext):
        """
        Executes all hooks that are registered for a specified lifecycle phase.

        This method iterates through the list of hooks registered for the given phase and calls each hook function.

        Args:
            phase (LifecyclePhase): The lifecycle phase during which to run the hooks (e.g., PRE_CONFIGURE, POST_CONFIGURE).
        """
        ctx.log(f"Running hooks for phase: {phase.value}")
        for hook in self._hooks.get(phase, []):
            hook_context = LifecycleHookContext(
                phase=phase, name=f"{hook.__name__} ({phase.value})"
            )
            ctx.add_child_ctx(hook_context)
            try:
                hook_context.start()
                hook(hook_context)
                if hook_context.status == ExecutionStatus.RUNNING:
                    hook_context.status = ExecutionStatus.SUCCESS
            except Exception as e:
                hook_context.status = ExecutionStatus.ERROR
                hook_context.error = e
                hook_context.log(f"Hook failed: {e}")
                break
            finally:
                hook_context.end()

    @abstractmethod
    def configure(self, ctx: TestStepContext):
        """Abstract method for configuring the component."""

    @abstractmethod
    def deploy(self, ctx: TestStepContext):
        """Abstract method for deploying the component (spawn a process or start a container/deployment)."""

    @abstractmethod
    def start(self, ctx: TestStepContext):
        """Abstract method for starting the component's execution behavior."""

    @abstractmethod
    def stop(self, ctx: TestStepContext):
        """Abstract method for stopping the component's execution behavior."""

    @abstractmethod
    def destroy(self, ctx: TestStepContext):
        """Abstract method for destroying the component (e.g. kill process, stop/remove container).

        The specific signals (term/kill) and container cleanup (stop vs rm) will be dictated and
        configured by the strategy implementation and lifecycle hooks.
        """

    @abstractmethod
    def start_monitoring(self, ctx: TestStepContext):
        """Abstract method to start monitoring the component."""

    @abstractmethod
    def stop_monitoring(self, ctx: TestStepContext):
        """Abstract method to stop monitoring the component."""

    @abstractmethod
    def collect_monitoring_data(self, ctx: TestExecutionContext):
        """Abstract method to collect monitoring data for the component."""
