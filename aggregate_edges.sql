select edge_id, sum(P) P, sum(30*E) E, sum(E) / sum(P) Tt, count(route_id) N, stddev_samp(P) stddev_P, stddev_samp(30*E) stddev_E into probaggedges from probedges group by edge_id having sum(P) > 0;
