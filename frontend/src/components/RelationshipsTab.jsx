import React from "react";

export default function RelationshipsTab({ data, loading, error }) {
    if (loading) {
        return <div className="object-details-empty">Loading relationships...</div>;
    }

    if (error) {
        return <div className="object-details-error">{error}</div>;
    }

    if (!data) {
        return <div className="object-details-empty">No relationships data available.</div>;
    }

    const {
        primary_keys = [],
        foreign_keys = [],
        referenced_by = [],
        pk_candidates = [],
    } = data;

    return (
        <div className="relationships-tab">
            <section className="relationships-section">
                <h3>Verified Primary Keys</h3>
                {primary_keys.length ? (
                    <div className="chip-list">
                        {primary_keys.map((pk) => (
                            <span key={pk} className="chip chip-verified">
                                {pk}
                            </span>
                        ))}
                    </div>
                ) : (
                    <div className="object-details-empty">No verified primary keys found.</div>
                )}
            </section>

            <section className="relationships-section">
                <h3>Inferred PK Candidates</h3>
                {pk_candidates.length ? (
                    <div className="relationship-table">
                        <div className="relationship-row relationship-header relationship-row-3">
                            <div>Column</div>
                            <div>Score</div>
                            <div>Reasons</div>
                        </div>

                        {pk_candidates.map((item) => (
                            <div className="relationship-row relationship-row-3" key={item.column}>
                                <div>{item.column}</div>
                                <div>{item.score}</div>
                                <div>{(item.reasons || []).join(", ")}</div>
                            </div>
                        ))}
                    </div>
                ) : (
                    <div className="object-details-empty">No inferred PK candidates found.</div>
                )}
            </section>

            <section className="relationships-section">
                <h3>Verified Foreign Keys</h3>
                {foreign_keys.length ? (
                    <div className="relationship-table">
                        <div className="relationship-row relationship-header relationship-row-3">
                            <div>Column</div>
                            <div>References</div>
                            <div>Confidence</div>
                        </div>

                        {foreign_keys.map((fk, index) => (
                            <div className="relationship-row relationship-row-3" key={`${fk.column}-${index}`}>
                                <div>{fk.column}</div>
                                <div>{fk.ref_schema}.{fk.ref_table}.{fk.ref_column}</div>
                                <div>{fk.confidence}</div>
                            </div>
                        ))}
                    </div>
                ) : (
                    <div className="object-details-empty">No verified foreign keys found.</div>
                )}
            </section>

            <section className="relationships-section">
                <h3>Referenced By</h3>
                {referenced_by.length ? (
                    <div className="relationship-table">
                        <div className="relationship-row relationship-header relationship-row-4">
                            <div>Schema</div>
                            <div>Table</div>
                            <div>Column</div>
                            <div>References Column</div>
                        </div>

                        {referenced_by.map((row, index) => (
                            <div
                                className="relationship-row relationship-row-4"
                                key={`${row.table}-${row.column}-${index}`}
                            >
                                <div>{row.schema}</div>
                                <div>{row.table}</div>
                                <div>{row.column}</div>
                                <div>{row.referenced_column}</div>
                            </div>
                        ))}
                    </div>
                ) : (
                    <div className="object-details-empty">No incoming references found.</div>
                )}
            </section>
        </div>
    );
}