# Safety

[Architecture overview](../ARCHITECTURE.md) · [Control Nodes](NODES.md) · [Client interface](CLIENTS.md)

During armed operations, the ground station is at the control point and reaches VECTOR over the wireless link. Losing that link can leave the pad powered and pressurized while the operator can no longer observe or command it. The system's failure response is distributed across hardware, node firmware, and VECTOR so a return to default states can originate locally at the layer that still works.

## Layers and responsibilities

| Failure or trigger | Responding layer | Intended response |
|---|---|---|
| A control node loses power | Electrical and pneumatic hardware | Solenoids de-energize; actuated valves lose pneumatic pressure and return to their spring-biased defaults. This does not require software or a network command. |
| A node receives no server packet for five minutes | Node firmware | Reset every control to its configured default. This covers loss of the node's link or a server that stops communicating. |
| VECTOR has no state-stream client for ten minutes | VECTOR GUI watchdog | Send QLCP ESTOP to registered nodes, which reset their controls to defaults. This covers sustained loss of the ground-station connection while VECTOR and the pad LAN are still running. |

These timers watch different signals and are not one combined countdown. The node watchdog measures time since its last received server packet, so regular server heartbeats keep it alive even when HELM is gone. The GUI watchdog supplies the additional response to that case. VECTOR's own short heartbeat checks remove unresponsive sessions; they are separate from the node's five-minute reset.

The [firmware packet handler](https://github.com/Queens-Rocket-Engineering-Team/ctl-node-firmware/blob/58786c6d79764e23c3087b8c2cc9cc35f549a270/main/packet_handler.c) defines the five-minute watchdog and calls each control's `set_default` method both on expiry and on ESTOP. The hardware power-loss behavior above is the team's described operating design; server tests do not exercise those physical mechanisms.

## What a default means

Safe states are specific to the plumbing and electrical design. A vent may need to open while a feed valve closes. Each node's CONFIG supplies `default_state` for its controls, and VECTOR exposes those values to clients. ESTOP asks the node to restore them; VECTOR does not calculate a safe valve configuration from sensor readings. It has no global armed-state or interlock model. See [PANDA's power architecture](https://github.com/Queens-Rocket-Engineering-Team/ctl-panda/blob/ce662421b9694f965393419c044766101c4ff2f3/README.md#power-architecture) for an example of the hardware safety boundary.

Control algorithms also stay local where needed. For example, the [GSE heater task](https://github.com/Queens-Rocket-Engineering-Team/ctl-node-firmware/blob/58786c6d79764e23c3087b8c2cc9cc35f549a270/boards/gse_node/heater_control.c) reads temperatures and drives heater power from a PID setpoint. VECTOR transports the setpoint and readings; the heater loop runs on the node. Older Kasa outlets have a separate control path and are not affected by QLCP ESTOP.

## The GUI watchdog

[GUIWatchdog](../src/vector/runtime/gui_watchdog.py) polls every five seconds and uses the [state stream's](../src/vector/runtime/state_stream.py) client count as its liveness signal. Its ten-minute window starts when the runtime is constructed, including a boot where no GUI ever connects.

On expiry it latches and attempts one ESTOP per connection key. A node that connects or reconnects while the watchdog remains tripped gets an ESTOP attempt too. A state client reconnecting rearms the watchdog; it does not restore previously commanded states. An operator must command the desired state again.

**Any open `/ws/state` connection keeps this watchdog from tripping.** VECTOR does not distinguish a controlling HELM instance from a view-only tablet. Field tablets are used during setup and are expected to be disconnected during armed operations, with personnel at the control point. A forgotten viewer on the pad LAN could otherwise remain connected after the radio link fails. REST polling and other WebSockets do not count as presence.

A successful ESTOP send means the TCP write succeeded. Nodes respond with STATUS, but VECTOR's ESTOP command record does not wait for that response, and the watchdog's counters report send outcomes. Device-reported states and physical feedback are separate observations; reconnecting, sending ESTOP, or receiving an acknowledgement alone does not establish that the complete system is safe to approach.

## Launch Canada context

For the system-level requirements behind these boundaries, see the [Launch Canada DTEG, revision 4](https://www.launchcanada.org/s/Launch-Canada-DTEG-R4.pdf), particularly §2.2.3.4.1 on fault-tolerant depressurization and §11.2.4 on launch-system fault tolerance. These requirements apply to the complete hardware and software system; VECTOR supplies only part of the response.

## Changing this subsystem

Review changes to client registration, connection keys, command sending, and device defaults against this failure-response model. In particular, changing what counts as a GUI client also changes the watchdog's trigger.

[Watchdog tests](../tests/unit/test_gui_watchdog.py) cover boot without a client, timeout, latching, reconnection, new node sessions, and send failures. [State-stream tests](../tests/unit/test_state_stream.py) cover client lifetime. These verify server behavior. Firmware watchdogs and physical default states require their own validation on the relevant hardware.
