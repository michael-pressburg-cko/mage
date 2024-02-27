use c_str_macro::c_str;
use rsmgp_sys::list::*;
use rsmgp_sys::memgraph::*;
use rsmgp_sys::mgp::*;
use rsmgp_sys::property::*;
use rsmgp_sys::result::*;
use rsmgp_sys::rsmgp::*;
use rsmgp_sys::value::*;
use rsmgp_sys::vertex::Vertex;
use rsmgp_sys::{close_module, define_optional_type, define_procedure, define_type, init_module};
use std::collections::HashSet;
use std::ffi::CString;
use std::os::raw::c_int;
use std::panic;

// Label == type
// Property name == name

init_module!(|memgraph: &Memgraph| -> Result<()> {
    memgraph.add_read_procedure(
        upsert_composite,
        c_str!("upsert_composite"),
        &[],
        &[
            define_optional_type!("strict", &MgpValue::make_bool(false, &memgraph)?, Type::Bool),
        ],
        &[
            define_type!("compositeName", Type::String),
            define_type!("nodes", Type::List, Type::Vertex),
        ],
    )?;

    Ok(())
});
// This procedure takes a node label (of the desired composite node) & a list of nodes, and will either find the matching node wherein all nodes
// share an edge (and only those nodes)
// If we can turn it into a write_procedure, we could create the composite & return that too
define_procedure!(upsert_composite, |memgraph: &Memgraph| -> Result<()> {
    let result = memgraph.result_record()?;
    let args = memgraph.args()?;
    let composite_type = args.value_at(0)?; // c_str
    let passed_nodes = args.value_at(1)?; // list<vertex>
    let mut matched_or_created_node: Option<Vertex> = None;
    if let Value::List(nodes) = passed_nodes {
        // Create a set of the names of all the nodes we want for quicker lookup
        let mut set = HashSet::new();
        let mut in_set = HashSet::new();
        let n_iter = nodes.iter()?;
        for n in n_iter {
            if let Value::Vertex(v) = n {
                if let Ok(prop) = v.property(c_str!("name")) {
                    set.insert(prop.name);
                }
            } else {
                return Err(Error::UnableToFindVertexById);
            }
        }

        if let Value::String(comp_str) = composite_type {
            if let Ok(vertex_iter) = memgraph.vertices_iter() {
                for vertex in vertex_iter {
                    let label_check = vertex.has_label(&comp_str);
                    if let Ok(true) = label_check {
                        let in_edges = vertex.in_edges();
                        if let Ok(edges) = in_edges {
                            for e in edges {
                                let src = e.from_vertex();
                                if let Ok(v) = src {
                                    if let Ok(prop) = v.property(c_str!("name")) {
                                        in_set.insert(prop.name);
                                    }
                                } else {
                                    return Err(Error::UnableToReturnVertexPropertiesIterator);
                                }
                            }
                        } else {
                            return Err(Error::UnableToReturnVertexInEdgesIterator);
                        }

                        // we are in the right node type, and we have build 2 sets.
                        // if in_set is a superset of set, then we have a match
                        // we then want to check if they are both supersets of eachother
                        // i.e. they are identical

                        if set.is_subset(&in_set) && set.is_superset(&in_set) {
                            // they match - write the node ID to the result record and return
                            matched_or_created_node = Some(vertex);
                        }
                    }
                    in_set.clear();
                }
            }
            // no matching node exists - create one that is a :COMPOSITE_OF all the passed nodes i, write the node ID to the result record and return
            // ... but if we cant define a write_procedure, just return nothing
            match matched_or_created_node {
                Some(v) => {
                    result.insert_vertex(c_str!("comp"), &v)?;
                }
                None => result.insert_null(c_str!("comp"))?,
            }
        } else {
            return Err(Error::UnableToMakeValueString);
        }
    }

    Ok(())
});

close_module!(|| -> Result<()> { Ok(()) });
