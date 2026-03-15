DBPort Product Story

Product Narrative

DBPort is the production layer for DuckDB-native data products.

It gives model builders a governed path from warehouse inputs to published outputs without forcing them into a specific modeling framework. Users can load versioned inputs, work in a ready-to-use DuckDB runtime, apply their own SQL or Python logic, validate results against a declared output contract, and publish datasets with confidence.

DBPort exists to solve a common problem in modern analytics and data science workflows: the middle of the pipeline is usually flexible, but the edges are brittle. Teams often have many ways to build models, analyses, and transformations, but no simple, reliable way to manage dataset inputs, output schemas, metadata, reproducibility, and publication. DBPort focuses exactly on those edges.

Its purpose is not to replace modeling tools, orchestration systems, or warehouse technologies. Its purpose is to make them easier to use together by owning the dataset lifecycle around them.

One-Sentence Positioning

DBPort helps teams build reproducible warehouse datasets by running models in DuckDB and managing the governed path from inputs to published outputs.

Tagline Options
	•	DBPort — build locally, publish safely.
	•	DBPort — the production layer for DuckDB data products.
	•	DBPort — governed inputs, flexible models, reliable outputs.
	•	DBPort — run your warehouse models on DuckDB, publish with confidence.

Why DBPort Exists

Many teams want the speed and simplicity of DuckDB for real analytical work, but production workflows still require more than a database engine:
	•	inputs must be loaded from a warehouse in a controlled way
	•	output schemas must be defined and enforced
	•	metadata and codelists should remain attached to the dataset
	•	input and output versions need to be tracked
	•	publishes must be safe, repeatable, and inspectable
	•	runs should be reproducible and suitable for periodic recompute

Without a dedicated layer for those concerns, each project ends up reimplementing them with custom scripts, ad hoc conventions, and fragile glue code.

DBPort turns those repeated patterns into one consistent product.

Core Promise

DBPort gives users three things:
	1.	A ready-to-use DuckDB runtime for local and production execution.
	2.	A governed dataset contract for inputs, outputs, schema, and metadata.
	3.	A reproducible publication workflow with version-aware state and safe publish semantics.

Product Scope

DBPort is deliberately opinionated about the edges of a data product workflow and deliberately unopinionated about the middle.

DBPort owns:
	•	connecting warehouse datasets to a local DuckDB runtime
	•	resolving and loading inputs
	•	declaring and enforcing output schemas
	•	attaching metadata and codelists
	•	tracking input and output versions
	•	publishing output datasets safely
	•	making runs reproducible and inspectable

DBPort does not prescribe:
	•	how users write their model logic
	•	whether they use SQL, Python, pandas, Polars, PyMC, or other tools
	•	how jobs are scheduled at the orchestration level
	•	how teams define broad transformation DAGs across many models

That separation is central to the product story.

The Mental Model

DBPort is the port where warehouse datasets come in, model logic runs, and governed outputs leave.

The user brings the model.
DBPort manages the dataset lifecycle.

This is the simplest and most durable way to understand the product.

Target Users

1. Data Product Builders

These users are responsible for producing datasets that others depend on. They need:
	•	reliable input loading
	•	clear output contracts
	•	metadata discipline
	•	reproducible recomputation
	•	safe publication into a warehouse

For them, DBPort removes platform friction and standardizes the repetitive parts of dataset production.

2. Applied Modelers

These users want to spend their time on methods, analysis, and model logic. They need:
	•	a fast local runtime
	•	easy access to warehouse inputs
	•	freedom in the middle of the workflow
	•	a simple path to push results back into production

For them, DBPort provides a governed execution environment without imposing a modeling framework.

3. Small Platform or Analytics Teams

These teams want stronger production guarantees than ad hoc notebooks and scripts, but they do not want to build or adopt a heavy internal platform. They need:
	•	repeatability
	•	visibility into versions and state
	•	safer publishing
	•	easier project bootstrapping

For them, DBPort acts as a lightweight productized runtime layer.

What Makes DBPort Different

DBPort is not just another DuckDB wrapper and not just another pipeline tool.

Its value comes from the combination of:
	•	DuckDB-native execution
	•	warehouse-connected inputs and outputs
	•	explicit output contracts
	•	metadata and codelist handling
	•	version-aware state
	•	resumable and idempotent publication
	•	a workflow that stays flexible in the middle

Most tools in the ecosystem cover only one part of this story.

Relationship to Other Tools

DBPort and DuckDB

DuckDB is the execution engine.
DBPort is the production workflow layer around it.

DuckDB provides fast local and embedded analytics. DBPort turns that capability into a repeatable dataset-building workflow by adding governed inputs, output contracts, and publish semantics.

DBPort and dbt

dbt is a strong partner for modeling and transformation logic.
DBPort does not compete with dbt’s core strengths.

Instead:
	•	dbt can own transformations in the middle
	•	DBPort can own dataset inputs, contracts, versions, and publication at the edges

This is a natural integration direction.

DBPort and Orchestrators

DBPort is not an orchestrator.
Tools like Dagster, Airflow, or other schedulers can trigger runs, but DBPort defines what a safe and reproducible run actually means at the dataset level.

DBPort and Warehouse Technologies

DBPort is not a warehouse.
It is the runtime and lifecycle layer that connects model execution in DuckDB to warehouse-backed datasets.

Product Vision

DBPort should become the default way to run warehouse-connected data product models on DuckDB in production.

The long-term vision is:
	•	a clear project structure
	•	a small and powerful client API
	•	a strong CLI for interactive control and inspection
	•	excellent visibility into state, versions, and publish behavior
	•	smooth integration with modeling and orchestration tools
	•	a developer experience that makes robust dataset production feel simple

Product Principles

1. Own the edges, not the middle

DBPort should remain focused on dataset lifecycle concerns.
Users should stay free to implement their model logic however they want.

2. Reproducibility must be visible

State, versions, loaded inputs, and publish history should be inspectable, not hidden.
The system should make it easy to understand what happened in a run and why.

3. Contracts before convenience

Fast workflows matter, but output contracts, metadata, and schema discipline are what make the system production-ready.

4. One good path is better than many vague ones

The product should favor a small number of obvious, well-supported workflows over a broad set of loosely defined features.

5. Production should feel local

Users should feel like they are working in a clean local analytical environment, while DBPort ensures the resulting workflow is fit for production.

Core Product Messages

Short Description

DBPort is a DuckDB-native runtime for building reproducible warehouse datasets.

Medium Description

DBPort helps teams run data product models on DuckDB with governed warehouse inputs, explicit output contracts, and safe publication workflows. It keeps the modeling layer flexible while making input resolution, schema management, metadata, and version-aware publishing reliable and repeatable.

Long Description

DBPort is the production layer for DuckDB-native data products. It gives teams a ready-to-use runtime for loading warehouse inputs into DuckDB, running custom model logic, validating outputs against declared contracts, and publishing datasets with version-aware state and reproducible semantics. Instead of prescribing how models should be built, DBPort focuses on the dataset lifecycle around them: inputs, schemas, metadata, codelists, run state, and publication. The result is a faster path from local development to reliable production datasets.

Ideal User Story

A user starts a project and declares an output dataset contract.
They load a set of governed warehouse inputs into DuckDB.
They build the model logic using the tools that make sense for the task.
They inspect intermediate results locally.
They validate the output against the declared contract.
They publish the result back to the warehouse.
The system records the state of the run, the versions of loaded inputs, and the version history of the output.
On the next run, they can recompute with confidence and understand exactly what changed.

The Problem Statement

Teams increasingly want to use DuckDB for serious analytical and model-driven workflows, but productionizing those workflows remains unnecessarily difficult.

The friction is not usually in the model logic itself. The friction is in everything around it:
	•	how inputs are loaded and versioned
	•	how outputs are declared and validated
	•	how metadata is preserved
	•	how reruns stay reproducible
	•	how publishes remain safe and inspectable

DBPort solves that operational gap.

The Value Proposition

DBPort provides a path from warehouse data to governed published datasets with minimal platform overhead.

It allows teams to:
	•	move faster than building custom infrastructure
	•	stay more flexible than adopting a heavy end-to-end platform
	•	keep DuckDB at the center of execution
	•	improve reproducibility and confidence in production runs
	•	standardize how datasets are built and published

Brand Narrative

DBPort represents a clear idea: a controlled point of exchange between model development and production data products.

At a port, inputs arrive, goods are processed, and outputs leave under managed conditions. That is exactly the role of DBPort in a data workflow. It is where governed datasets enter a local analytical runtime, where transformations and models can happen, and where validated outputs are prepared for safe publication.

The name conveys movement, control, and reliability without sounding like a generic toolbox.

Tone and Identity

DBPort should sound:
	•	practical
	•	dependable
	•	technical but not academic
	•	focused on production reliability
	•	compatible with modern analytics and modeling workflows

It should avoid sounding:
	•	like a generic utilities package
	•	like a heavyweight platform
	•	like a narrow ETL framework
	•	like a purely ML-specific tool

Documentation Structure Recommendation

1. Introduction
	•	What DBPort is
	•	What problem it solves
	•	Who it is for
	•	How it fits with DuckDB, dbt, and orchestration tools

2. Quickstart
	•	install
	•	initialize project
	•	define output schema
	•	load inputs
	•	run logic
	•	publish output

3. Concepts
	•	inputs
	•	outputs
	•	contracts
	•	metadata
	•	versions
	•	lock state
	•	publication semantics

4. Core API
	•	client
	•	schema declaration
	•	load
	•	execute
	•	publish
	•	status and inspection

5. CLI
	•	init
	•	status
	•	load
	•	run
	•	publish
	•	diff
	•	validate

6. Integrations
	•	dbt
	•	orchestration tools
	•	warehouse backends

7. Reliability Model
	•	reproducibility
	•	idempotency
	•	schema drift checks
	•	resumability
	•	version history

8. Examples
	•	pure SQL model
	•	Python data workflow
	•	Bayesian modeling workflow
	•	periodic recompute workflow

Suggested Homepage Copy

Hero

DBPort

The production layer for DuckDB data products.

Run warehouse-connected models on DuckDB with governed inputs, explicit output contracts, and safe publication workflows.

Subheading

Bring your own model. DBPort manages the dataset lifecycle.

Key Points
	•	Load governed warehouse inputs into DuckDB
	•	Define and enforce output contracts
	•	Track versions and run state
	•	Publish datasets safely and reproducibly
	•	Integrate with your preferred modeling tools

FAQ Positioning Answers

Is DBPort an orchestrator?

No. DBPort defines dataset lifecycle behavior and safe publication semantics. It can be run by orchestrators, but it is not one.

Is DBPort a transformation framework?

Not primarily. DBPort leaves transformation and modeling logic flexible. It focuses on inputs, outputs, contracts, state, and publication.

Does DBPort replace dbt?

No. dbt is complementary. dbt can be an excellent choice for transformations in the middle, while DBPort manages dataset lifecycle concerns at the edges.

Why not just use DuckDB directly?

DuckDB is the execution engine, but it does not by itself define reproducible warehouse input loading, output contracts, metadata handling, version-aware state, or safe publication semantics. DBPort adds those production capabilities.

Launch Positioning

DBPort is for teams that want the speed of DuckDB without sacrificing production discipline.

It is the simplest way to move from warehouse inputs to published datasets with a workflow that is:
	•	fast enough for development
	•	structured enough for production
	•	flexible enough for real model work

Final Positioning Statement

DBPort is a DuckDB-native runtime and lifecycle layer for reproducible warehouse datasets. It helps teams load governed inputs, run their own models, enforce output contracts, and publish data products safely.

Rename Rollout Notes

When replacing lmi_tools with DBPort, the documentation should consistently reinforce the new identity:
	•	replace references to “tools” with “runtime”, “product”, or “SDK” depending on context
	•	shift messaging away from project origin history and toward product purpose
	•	explain the name once, then let the product story carry it
	•	keep technical examples concrete and centered on inputs, model logic, and publication
	•	use consistent wording for contracts, versions, state, and publish semantics

Recommended Short Elevator Pitch

DBPort is the production layer for DuckDB data products. It connects warehouse inputs to flexible model execution and safe, version-aware dataset publication.

Recommended Team Description

We are building DBPort to make DuckDB a practical foundation for reproducible data products in production. DBPort gives teams a governed way to load inputs, run custom models, and publish outputs safely without building a heavyweight internal platform.

Closing Message for the Docs

DBPort exists so that teams can focus on the logic that makes their datasets valuable, while the system takes care of the workflow that makes those datasets reliable.