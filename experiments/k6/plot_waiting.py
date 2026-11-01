#!/usr/bin/env python3
"""Render reproducible waiting-overhead A/B figures from retained raw reports."""
import argparse
import csv
import json
from pathlib import Path
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

COLORS = {'polling':'#c56b38', 'notification':'#247c94'}
LABELS = {'polling':'A: polling', 'notification':'B: notification'}
PHASES = ['normal','overload','recovery']


def load_reports(root):
    comparison = json.loads((root/'comparison.json').read_text())
    if not comparison['complete']:
        raise ValueError('Refusing to plot an incomplete experiment as a final comparison')
    reports = []
    for item in comparison['runs']:
        path = Path(item['report'])
        if not path.exists(): path = root/'runs'/path.parent.name/'report.json'
        reports.append((item,json.loads(path.read_text()),path.parent))
    return comparison,reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results',type=Path)
    args = parser.parse_args()
    root=args.results.resolve()
    comparison,reports = load_reports(root)
    records=[]
    for item,report,folder in reports:
        for row in report['measurements']:
            if row['phase']=='warmup': continue
            record={k:row.get(k) for k in ['phase','target_rps','sent','finished','successes','failures',
                'valid_load','generator_dropped','rejected_503','deadline_504','success_p95_ms',
                'api_cpu_seconds','resource_window_seconds','api_peak_rss_mib','api_max_threads',
                'completion_rps_over_execution']}
            record.update(variant=item['variant'],repeat=item['repeat'],purpose=item['purpose'])
            record['api_cpu_ms_per_completed_request']=1000*row['api_cpu_seconds']/max(1,row['finished'])
            record['failure_percent']=100*row['failures']/max(1,row['finished'])
            record['success_p99_ms']=row['metrics'].get('successful_latency_ms',{}).get('values',{}).get('p(99)')
            record['mismatches']=row['metrics'].get('result_mismatches',{}).get('values',{}).get('count',0)
            records.append(record)
    if any(not r['valid_load'] for r in records):
        raise ValueError('Generator did not deliver at least one stage; retain it as invalid, do not aggregate')
    if any(r['mismatches'] for r in records): raise ValueError('Result mismatches invalidate the comparison')
    with (root/'comparison.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(records[0]))
        writer.writeheader();writer.writerows(records)
    measured=[r for r in records if r['purpose']=='comparison']
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,
                         'figure.facecolor':'#ffffff','axes.facecolor':'#ffffff'})
    fig,axes=plt.subplots(2,2,figsize=(12,8.5))
    specs=[('api_cpu_ms_per_completed_request','API CPU / completed request','CPU ms/request; includes drain and telemetry'),
           ('api_peak_rss_mib','API sampled peak resident memory','MiB; sampled every 2 seconds'),
           ('success_p95_ms','Successful-request P95','Milliseconds; logarithmic scale'),
           ('failure_percent','Failed / rejected requests','Percent of completed requests')]
    aggregate={}
    for ax,(key,title,ylabel) in zip(axes.flat,specs):
        x=np.arange(len(PHASES))
        for offset,variant in [(-.18,'polling'),(.18,'notification')]:
            groups=[[float(r[key]) for r in measured if r['variant']==variant and r['phase']==phase] for phase in PHASES]
            med=np.array([statistics.median(values) for values in groups])
            low=med-np.array([min(values) for values in groups]);high=np.array([max(values) for values in groups])-med
            ax.bar(x+offset,med,width=.33,color=COLORS[variant],label=LABELS[variant],alpha=.86,
                   yerr=np.array([low,high]),capsize=4)
            for index,values in enumerate(groups):
                ax.scatter(np.full(len(values),x[index]+offset),values,s=15,color='#23313d',zorder=3)
            aggregate.setdefault(variant,{})[key]={phase:statistics.median(values) for phase,values in zip(PHASES,groups)}
        ax.set_xticks(x,['Normal\n10 RPS','Overload\n50 RPS','Recovery\n10 RPS'])
        ax.set_title(title,loc='left',fontweight='bold');ax.set_ylabel(ylabel)
        ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
        if key=='success_p95_ms': ax.set_yscale('log')
        if key=='failure_percent' and all(r[key]==0 for r in measured):
            ax.set_ylim(0,1);ax.text(.5,.5,'0 failures in both variants',transform=ax.transAxes,ha='center')
    axes[0,0].legend(frameon=False)
    fig.suptitle('Synchronous request waiting: polling vs completion notification',fontweight='bold',fontsize=15)
    fig.text(.5,.015,'3 repetitions per variant; bars = median, whiskers = min/max, dots = individual trials.\n'
             'Same 2 simulated workers, batch=1, queue=1000. Local shared-host test; not production/model capacity.',ha='center',fontsize=9)
    fig.tight_layout(rect=[0,.065,1,.95])
    fig.savefig(root/'waiting-comparison.png',dpi=170)
    fig.savefig(root/'waiting-comparison.pdf')
    plt.close(fig)
    fig,axes=plt.subplots(3,1,figsize=(12,9),sharex=True)
    for item,report,folder in reports:
        if item['purpose']!='comparison' or item['repeat']!=0: continue
        rows=[]
        for stage in report['measurements']:
            if stage['phase']=='warmup': continue
            path=folder/Path(stage['path']).name/'api-resources.json'
            rows.extend(json.loads(path.read_text()))
        rows.sort(key=lambda row:row['epoch_s'])
        beginning=rows[0]['epoch_s']
        x=[row['epoch_s']-beginning for row in rows]
        pending=[row['inference_api_queue_depth']+row['inference_scheduler_pending_tasks']+
                 row['inference_worker_queued_batches'] for row in rows]
        variant=item['variant'];kwargs={'color':COLORS[variant],'label':LABELS[variant]}
        axes[0].plot(x,pending,**kwargs)
        axes[1].plot(x,[row['rss_mib'] for row in rows],**kwargs)
        axes[2].plot(x,[row['cpu_seconds']-rows[0]['cpu_seconds'] for row in rows],**kwargs)
    for ax,label in zip(axes,['Waiting tasks (batch=1)','API resident memory (MiB)','Cumulative API CPU seconds']):
        ax.set_ylabel(label);ax.grid(alpha=.18);ax.legend(frameon=False)
    axes[-1].set_xlabel('Elapsed seconds from normal-stage resource window; includes drain and stage gaps')
    fig.suptitle('Representative first pair: queue growth, memory and API CPU',fontweight='bold')
    fig.tight_layout(rect=[0,0,1,.96]);fig.savefig(root/'waiting-timeseries.png',dpi=170)
    fig.savefig(root/'waiting-timeseries.pdf');plt.close(fig)
    guard=[r for r in records if r['purpose']=='admission_guard' and r['phase']=='overload']
    if guard:
        item=guard[0]
        fig,axes=plt.subplots(1,2,figsize=(10,4.5))
        axes[0].bar(['Successful','503 refused','Other failures'],
                    [item['successes'],item['rejected_503'],item['failures']-item['rejected_503']],
                    color=['#247c94','#c56b38','#9d3e3e'])
        axes[0].set_ylabel('Requests');axes[0].set_title('Controlled refusal at limit=32',loc='left')
        for reference,report,folder in reports:
            if reference['purpose']!='admission_guard': continue
            for stage in report['measurements']:
                if stage['phase']!='overload': continue
                rows=json.loads((folder/Path(stage['path']).name/'api-resources.json').read_text())
                axes[1].plot([r['epoch_s']-rows[0]['epoch_s'] for r in rows],
                             [r['inference_sync_requests_active'] for r in rows],color='#247c94')
                axes[1].axhline(32,color='#c56b38',linestyle='--',label='Configured limit')
        axes[1].set_ylabel('Active synchronous handlers');axes[1].set_xlabel('Elapsed resource-window seconds')
        axes[1].legend(frameon=False);axes[1].set_title('Admission stays bounded',loc='left')
        fig.suptitle('Separate protection check: 50 RPS, 15 seconds, 2 workers',fontweight='bold')
        fig.text(.5,.025,'Different admission limit from A/B: lower latency here must not be claimed as an equal-success improvement.',ha='center',fontsize=9)
        fig.tight_layout(rect=[0,.07,1,.93]);fig.savefig(root/'admission-guard.png',dpi=170)
        fig.savefig(root/'admission-guard.pdf');plt.close(fig)
    (root/'aggregate.json').write_text(json.dumps(aggregate,indent=2))
    print(json.dumps(aggregate,indent=2))
    print('FIGURE_PATH='+str(root/'waiting-comparison.png'))


if __name__=='__main__': main()
