import argparse
from pathlib import Path

import dash
import pandas as pd
import plotly.express as px
from dash import dcc, html

from .NemLoader import NemLoader
from .SymbolLoader import SymbolLoader
from .VersionSummary import VersionSummary, group_version_summaries_by_tag


class WebApp:
    def __init__(self, resources_path):
        self.resources_path = Path(resources_path)

        self.nem_loader = None
        self.symbol_loader = None

        self.app = dash.Dash(__name__)

    def reload(self):
        self.nem_loader = NemLoader(self.resources_path / 'nemharvesters.csv')
        self.nem_loader.load()

        self.symbol_loader = SymbolLoader(self.resources_path / '250k.csv', {
            'AllNodes': self.resources_path / 'symbol_allnodes_addresses.txt',
            'NGL': self.resources_path / 'symbol_ngl_addresses.txt',
        })
        self.symbol_loader.load()

    def make_symbol_figure(self):
        version_summaries = self.symbol_loader.aggregate(lambda descriptor: descriptor.is_voting and 'NGL' not in descriptor.categories)
        return self._make_bar_graph(version_summaries, 'Cyprus Hardfork Progress', 0.67, {
            'for': 'green', 'against': 'red'
        })

    def make_nem_figure(self):
        version_summaries = self.nem_loader.aggregate()
        return self._make_bar_graph(version_summaries, 'Harlock Hardfork Progress', 0.5, {
            'for': 'green', 'delegating / updating': 'yellow', 'against': 'red'
        })

    @staticmethod
    def _make_bar_graph(version_summaries, title, threshold, key_color_map):
        grouped_version_summaries = group_version_summaries_by_tag(version_summaries.values())
        aggregate_version_summary = VersionSummary.aggregate_all(grouped_version_summaries.values())

        data_vectors = {'key': [], 'measure': [], 'value': [], 'percentage': []}
        for key in key_color_map:
            for measure in ['count', 'power']:
                data_vectors['key'].append(key)
                data_vectors['measure'].append(measure)
                data_vectors['value'].append(getattr(grouped_version_summaries[key], measure))
                data_vectors['percentage'].append(data_vectors['value'][-1] / getattr(aggregate_version_summary, measure))

        data_frame = pd.DataFrame(data_vectors)
        figure = px.bar(
            data_frame,
            x='measure',
            y='percentage',
            color='key',
            text='value',
            title=title,
            color_discrete_map=key_color_map)
        figure.add_hline(col=1, y=threshold, line_dash='dot')
        return figure

    def layout(self):
        symbol_figure = self.make_symbol_figure()
        nem_figure = self.make_nem_figure()

        self.app.layout = html.Div(children=[
            dcc.Graph(id='cyprus-graph', figure=symbol_figure),
            dcc.Graph(id='harlock-graph', figure=nem_figure)
        ])

    def run(self):
        self.app.run_server(debug=True)


def main():
    parser = argparse.ArgumentParser(description='processes data files')
    parser.add_argument('--resources', help='directory containing resources', required=True)
    args = parser.parse_args()

    app = WebApp(args.resources)
    app.reload()
    app.layout()
    app.run()


if '__main__' == __name__:
    main()
