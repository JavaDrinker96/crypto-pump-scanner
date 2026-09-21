"""Pure helpers for position protection and risk calculations."""


def allocate_tp_quantities(total, minimum, step, tp1_pct=.35, tp2_pct=.35):
    """Return valid TP1/TP2/TP3 quantities that exactly sum to total."""
    step=float(step or minimum or 1.0)
    minimum=float(minimum or step)
    total_steps=max(0,int((float(total)/step)+1e-9))
    min_steps=max(1,int(round(minimum/step)))
    if total_steps<min_steps:
        raise ValueError(f'position below minimum: total={total} minimum={minimum} step={step}')
    if total_steps < 2*min_steps:
        steps=(total_steps,0,0)
    elif total_steps < 3*min_steps:
        steps=(min_steps,total_steps-min_steps,0)
    else:
        n1=max(min_steps,int(total_steps*float(tp1_pct)))
        n2=max(min_steps,int(total_steps*float(tp2_pct)))
        if n1+n2>total_steps-min_steps:
            n1=min_steps; n2=min_steps
        n3=total_steps-n1-n2
        if n3<min_steps:
            n3=min_steps
            n2=max(min_steps,total_steps-n1-n3)
        steps=(n1,n2,n3)
    qty=tuple(n*step for n in steps)
    if abs(sum(qty)-total_steps*step)>step*0.01:
        raise ValueError(f'TP allocation mismatch: total={total} qty={qty} step={step}')
    return qty


def breakeven_stop(entry, side, current_stop, fee_buffer_pct=.06):
    """Return a stop that covers entry plus configurable fee buffer without loosening."""
    buffer=float(fee_buffer_pct)/100
    candidate=float(entry)*(1+buffer if side=='long' else 1-buffer)
    if side=='long':
        return max(float(current_stop),candidate)
    return min(float(current_stop),candidate)
