import type {
	ICredentialsDecrypted,
	ICredentialTestFunctions,
	IDataObject,
	INodeCredentialTestResult,
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
} from 'n8n-workflow';
import {
	NodeConnectionTypes,
	NodeOperationError,
	OperationalError,
	jsonParse,
	sleep,
} from 'n8n-workflow';

type ConnectionStatus =
	| 'connecting'
	| 'connected'
	| 'disconnected'
	| 'error'
	| 'reconnecting'
	| 'stopped';

interface RetryConfig {
	initialDelay: number;
	maxDelay: number;
	backoffFactor: number;
	maxAttempts: number;
	addJitter: boolean;
	honorServerRetry: boolean;
}

// 4xx responses mean the request itself is wrong, so repeating it unchanged cannot succeed. The two
// exceptions are explicitly transient: the server asked us to wait (429) or timed the request out.
const isRetryableStatus = (status: number) =>
	status < 400 || status >= 500 || status === 408 || status === 429;

// Any of these parameters can be driven by an expression, which may resolve to undefined or a
// non-numeric string. An unguarded NaN would make every computed delay NaN, and a NaN deadline never
// elapses, so the retry wait would spin at full speed instead of pausing.
const toFiniteNumber = (value: unknown, fallback: number, minimum = 0) => {
	const parsed = Number(value);
	return Number.isFinite(parsed) && parsed >= minimum ? parsed : fallback;
};

// A delay of zero is reachable both by configuration (the delay fields allow 0) and by coercion
// (`Number(null)` is 0). Without a floor the retry loop becomes a hot loop that hammers the endpoint
// as fast as the event loop allows, so every wait is at least this long.
const MIN_RETRY_DELAY_MS = 50;

export class ResilientSseTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Resilient SSE Trigger',
		name: 'resilientSseTrigger',
		icon: { light: 'file:resilient_sse.svg', dark: 'file:resilient_sse.dark.svg' },
		group: ['trigger'],
		version: [1],
		subtitle: '={{$parameter["url"]}}',
		description: 'Starts the workflow when Server-Sent Events are received, with automatic reconnect',
		eventTriggerDescription: 'Waiting for Server-Sent Events',
		activationMessage: 'You can now receive Server-Sent Events from your endpoint.',
		defaults: {
			name: 'Resilient SSE Trigger',
		},
		triggerPanel: {
			header: 'Streaming events from your SSE endpoint',
			executionsHelp: {
				inactive:
					'Nothing is connected yet. Select "Execute step" below to open the stream and watch events arrive.',
				active:
					'The stream is open and every event becomes one item. Deactivate the workflow to close it.',
			},
		},
		inputs: [],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: 'sseBearerApi',
				required: true,
				testedBy: 'sseBearerApiTest',
				displayOptions: {
					show: {
						authentication: ['bearerAuth'],
					},
				},
			},
			{
				name: 'sseHeaderApi',
				required: true,
				testedBy: 'sseHeaderApiTest',
				displayOptions: {
					show: {
						authentication: ['headerAuth'],
					},
				},
			},
		],
		properties: [
			{
				displayName: 'URL',
				name: 'url',
				type: 'string',
				default: '',
				placeholder: 'http://example.com/events',
				description: 'The URL to receive the SSE from',
				required: true,
			},
			{
				displayName: 'Authentication',
				name: 'authentication',
				type: 'options',
				options: [
					{
						name: 'Bearer Auth',
						value: 'bearerAuth',
					},
					{
						name: 'Header Auth',
						value: 'headerAuth',
					},
					{
						name: 'None',
						value: 'none',
					},
				],
				default: 'none',
				description: 'The way to authenticate with the SSE endpoint',
			},
			{
				displayName: 'Auto Reconnect',
				name: 'autoReconnect',
				type: 'boolean',
				default: true,
				description:
					'Whether to reconnect automatically when the stream ends, the connection drops, or the server returns an error',
			},
			{
				displayName:
					'The connection status is written to the n8n logs on every state change. Add "Emit Connection Status" below to receive it as workflow items as well.',
				name: 'statusNotice',
				type: 'notice',
				default: '',
			},
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				options: [
					{
						displayName: 'Add Jitter',
						name: 'addJitter',
						type: 'boolean',
						default: true,
						description:
							'Whether to randomise each retry delay between 50% and 100% of its computed value, so that many workflows reconnecting at once do not hit the server in lockstep',
					},
					{
						displayName: 'Backoff Factor',
						name: 'backoffFactor',
						type: 'number',
						default: 2,
						typeOptions: {
							minValue: 1,
							numberPrecision: 2,
						},
						description:
							'Multiplier applied to the retry delay after each failed attempt. Use 1 to keep the delay constant.',
					},
					{
						displayName: 'Connection Timeout (Ms)',
						name: 'connectionTimeout',
						type: 'number',
						default: 30000,
						typeOptions: {
							minValue: 0,
						},
						description:
							'How long to wait for the server to respond before treating the attempt as failed. Set to 0 to wait indefinitely. This does not limit how long an established stream stays open.',
					},
					{
						displayName: 'Emit Connection Status',
						name: 'emitStatus',
						type: 'boolean',
						default: false,
						description:
							'Whether to emit an extra item every time the connection state changes. Each emitted item starts its own workflow execution, so leave this off unless you are diagnosing a connection problem.',
					},
					{
						displayName: 'Headers',
						name: 'headers',
						type: 'fixedCollection',
						typeOptions: {
							multipleValues: true,
						},
						default: {},
						placeholder: 'Add Header',
						description: 'Extra headers to send with the request, alongside any authentication',
						options: [
							{
								name: 'parameters',
								displayName: 'Header',
								values: [
									{
										displayName: 'Name',
										name: 'name',
										type: 'string',
										default: '',
										description: 'Name of the header',
									},
									{
										displayName: 'Value',
										name: 'value',
										type: 'string',
										default: '',
										description: 'Value of the header',
									},
								],
							},
						],
					},
					{
						displayName: 'Headers (JSON)',
						name: 'headersJson',
						type: 'json',
						default: '{}',
						description:
							'Extra headers as a JSON object, for when the whole set comes from one expression. Merged with Headers above, which wins on conflict.',
					},
					{
						displayName: 'Honor Server Retry Hint',
						name: 'honorServerRetry',
						type: 'boolean',
						default: true,
						description:
							'Whether to use the delay the server sends in a "retry:" field instead of the computed backoff delay. The delay is still capped by Max Retry Delay.',
					},
					{
						displayName: 'Include Metadata',
						name: 'includeMetadata',
						type: 'boolean',
						default: true,
						description:
							'Whether to add a $metadata key to each item with the event type, last event ID, origin URL, and receive timestamp',
					},
					{
						displayName: 'Initial Retry Delay (Ms)',
						name: 'initialRetryDelay',
						type: 'number',
						default: 1000,
						typeOptions: {
							minValue: 0,
						},
						description: 'How long to wait before the first reconnect attempt',
					},
					{
						displayName: 'Max Retry Attempts',
						name: 'maxRetryAttempts',
						type: 'number',
						default: 0,
						typeOptions: {
							minValue: 0,
						},
						description:
							'How many consecutive attempts to make before giving up. Set to 0 to retry forever. The counter resets whenever a connection succeeds.',
					},
					{
						displayName: 'Max Retry Delay (Ms)',
						name: 'maxRetryDelay',
						type: 'number',
						default: 30000,
						typeOptions: {
							minValue: 0,
						},
						description: 'Upper bound for the retry delay once the backoff factor has been applied',
					},
				],
			},
		],
	};

	// These credentials carry no service URL of their own — the endpoint lives on the node — so there
	// is nothing to send a probe request to. They are checked for completeness instead, and the
	// message says so rather than implying the endpoint accepted them.
	methods = {
		credentialTest: {
			async sseBearerApiTest(
				this: ICredentialTestFunctions,
				credential: ICredentialsDecrypted,
			): Promise<INodeCredentialTestResult> {
				const token = credential.data?.token as string | undefined;

				if (!token?.trim()) {
					return { status: 'Error', message: 'Enter a bearer token' };
				}

				return {
					status: 'OK',
					message:
						'Token is set. It can only be confirmed once the trigger connects to your SSE endpoint.',
				};
			},

			async sseHeaderApiTest(
				this: ICredentialTestFunctions,
				credential: ICredentialsDecrypted,
			): Promise<INodeCredentialTestResult> {
				const name = credential.data?.name as string | undefined;
				const value = credential.data?.value as string | undefined;

				if (!name?.trim()) {
					return { status: 'Error', message: 'Enter the header name' };
				}
				if (!value?.trim()) {
					return { status: 'Error', message: 'Enter the header value' };
				}

				return {
					status: 'OK',
					message: `Header "${name}" is set. It can only be confirmed once the trigger connects to your SSE endpoint.`,
				};
			},
		},
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const node = this.getNode();
		const url = this.getNodeParameter('url') as string;
		const authentication = this.getNodeParameter('authentication', 'none') as string;
		const autoReconnect = this.getNodeParameter('autoReconnect') as boolean;

		// Unset collection entries arrive as undefined, so each fallback below must match the `default`
		// declared for that option or the behaviour changes depending on whether the user added the field.
		const additionalFields = this.getNodeParameter('additionalFields', {}) as {
			addJitter?: boolean;
			backoffFactor?: number;
			connectionTimeout?: number;
			emitStatus?: boolean;
			headers?: { parameters?: Array<{ name: string; value: string }> };
			headersJson?: string;
			honorServerRetry?: boolean;
			includeMetadata?: boolean;
			initialRetryDelay?: number;
			maxRetryAttempts?: number;
			maxRetryDelay?: number;
		};

		const connectionTimeout = toFiniteNumber(additionalFields.connectionTimeout, 30000);
		const includeMetadata = additionalFields.includeMetadata ?? true;
		const emitStatus = additionalFields.emitStatus ?? false;

		if (!url?.trim()) {
			throw new NodeOperationError(node, 'No URL provided', {
				description: 'Set the URL of the SSE endpoint you want to connect to',
			});
		}

		try {
			new URL(url);
		} catch {
			throw new NodeOperationError(node, `The URL "${url}" is not valid`, {
				description: 'Enter a fully qualified URL, for example http://example.com/events',
			});
		}

		const retryConfig: RetryConfig | undefined = autoReconnect
			? {
					initialDelay: toFiniteNumber(additionalFields.initialRetryDelay, 1000),
					maxDelay: toFiniteNumber(additionalFields.maxRetryDelay, 30000),
					backoffFactor: toFiniteNumber(additionalFields.backoffFactor, 2, 1),
					maxAttempts: toFiniteNumber(additionalFields.maxRetryAttempts, 0),
					addJitter: additionalFields.addJitter ?? true,
					honorServerRetry: additionalFields.honorServerRetry ?? true,
				}
			: undefined;

		let closed = false;
		let attempt = 0;
		let lastEventId = '';
		let serverRetryDelay: number | undefined;
		let abortController: AbortController | undefined;
		// Set when an attempt failed in a way that repeating it cannot fix, so the loop stops instead of
		// retrying on a schedule forever.
		let fatalReason: string | undefined;

		const reportStatus = (status: ConnectionStatus, message?: string) => {
			this.logger.info(
				`[Resilient SSE Trigger] ${node.name}: ${status}${message ? ` - ${message}` : ''}`,
				{ url, attempt },
			);

			if (emitStatus) {
				this.emit([
					this.helpers.returnJsonArray([
						{ status, url, attempt, message, timestamp: new Date().toISOString() },
					]),
				]);
			}
		};

		// Rebuilt per attempt so that a credential rotated between reconnects is picked up.
		const buildHeaders = async () => {
			const headers: Record<string, string> = {
				Accept: 'text/event-stream',
				'Cache-Control': 'no-cache',
			};

			if (authentication === 'bearerAuth') {
				const credentials = await this.getCredentials('sseBearerApi');
				headers.Authorization = `Bearer ${credentials.token as string}`;
			} else if (authentication === 'headerAuth') {
				const credentials = await this.getCredentials('sseHeaderApi');
				headers[credentials.name as string] = credentials.value as string;
			}

			// JSON is applied first so that an explicitly named header always wins over a bulk object.
			if (additionalFields.headersJson) {
				const parsed = jsonParse<IDataObject>(additionalFields.headersJson, {
					errorMessage: 'Headers (JSON) is not valid JSON',
				});
				for (const [key, value] of Object.entries(parsed)) {
					headers[key] = String(value);
				}
			}

			for (const header of additionalFields.headers?.parameters ?? []) {
				if (header.name) headers[header.name] = header.value;
			}

			// Set last so that a custom header cannot break resumption after a reconnect.
			if (lastEventId) headers['Last-Event-ID'] = lastEventId;

			return headers;
		};

		// The delay grows geometrically per consecutive failure. A server `retry:` hint, when honored,
		// replaces the computed value but is still capped, so a misbehaving server cannot stall the
		// trigger indefinitely.
		const nextRetryDelay = (config: RetryConfig) => {
			const computed =
				config.honorServerRetry && serverRetryDelay !== undefined
					? serverRetryDelay
					: config.initialDelay * Math.pow(config.backoffFactor, attempt - 1);

			const capped = Math.min(computed, config.maxDelay);
			const jittered = config.addJitter ? capped / 2 + Math.random() * (capped / 2) : capped;
			return Math.max(MIN_RETRY_DELAY_MS, toFiniteNumber(jittered, config.initialDelay));
		};

		// Slept in short slices rather than one long pause so that deactivating the workflow tears the
		// trigger down promptly instead of waiting out the full backoff delay.
		const waitInSlices = async (delay: number, stop: () => boolean) => {
			// A NaN deadline would never elapse, turning this into a busy loop.
			const deadline = Date.now() + toFiniteNumber(delay, 0);
			while (!stop()) {
				const remaining = deadline - Date.now();
				if (remaining <= 0) return;
				await sleep(Math.min(250, remaining));
			}
		};

		const decoder = new TextDecoder();
		let buffer = '';
		let dataLines: string[] = [];
		let eventName = '';

		const dispatchEvent = () => {
			if (dataLines.length === 0) {
				eventName = '';
				return;
			}

			const raw = dataLines.join('\n');
			const eventType = eventName || 'message';
			dataLines = [];
			eventName = '';

			// A non-JSON payload (a plain-text heartbeat, a `[DONE]` sentinel) must not tear the stream
			// down, so it is passed through under `data` rather than thrown. Arrays and scalars are
			// wrapped for the same reason: spreading them would produce numeric keys.
			let payload: IDataObject;
			try {
				const parsed = jsonParse<unknown>(raw);
				payload =
					typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
						? { ...(parsed as IDataObject) }
						: ({ data: parsed } as IDataObject);
			} catch {
				payload = { data: raw };
			}

			if (includeMetadata) {
				payload.$metadata = {
					eventType,
					lastEventId,
					origin: url,
					timestamp: new Date().toISOString(),
				};
			}

			this.emit([this.helpers.returnJsonArray([payload])]);
		};

		const handleLine = (line: string) => {
			if (line === '') {
				dispatchEvent();
				return;
			}
			if (line.startsWith(':')) return;

			const separator = line.indexOf(':');
			const field = separator === -1 ? line : line.slice(0, separator);
			let value = separator === -1 ? '' : line.slice(separator + 1);
			if (value.startsWith(' ')) value = value.slice(1);

			switch (field) {
				case 'data':
					dataLines.push(value);
					break;
				case 'event':
					eventName = value;
					break;
				case 'id':
					if (!value.includes('\0')) lastEventId = value;
					break;
				case 'retry':
					if (/^\d+$/.test(value)) serverRetryDelay = Number(value);
					break;
				default:
					break;
			}
		};

		// A trailing '\r' is held back because it may be the first half of a CRLF pair that lands in the
		// next chunk; treating it as a terminator would dispatch the event one line early.
		const consumeBuffer = () => {
			let searchable = buffer;
			let heldBack = '';
			if (searchable.endsWith('\r')) {
				heldBack = '\r';
				searchable = searchable.slice(0, -1);
			}

			const lines = searchable.split(/\r\n|\r|\n/);
			const remainder = lines.pop() ?? '';
			for (const line of lines) handleLine(line);
			buffer = remainder + heldBack;
		};

		const readStream = async () => {
			const controller = new AbortController();
			abortController = controller;

			// A credential that cannot be resolved (deleted, or missing a required field) is as
			// unrecoverable as a rejected request, so it must not be retried on a schedule.
			const headers = await buildHeaders().catch((error: unknown) => {
				fatalReason = `the credential could not be loaded (${
					error instanceof Error ? error.message : String(error)
				})`;
				throw error;
			});

			let responseSettled = false;
			let timedOut = false;

			const fetchPromise = fetch(url, { headers, signal: controller.signal });
			void fetchPromise.then(
				() => {
					responseSettled = true;
				},
				() => {
					responseSettled = true;
				},
			);

			// Guards only the wait for response headers; once the stream is open it may stay open
			// indefinitely, which is the whole point of SSE.
			if (connectionTimeout > 0) {
				void (async () => {
					await waitInSlices(connectionTimeout, () => responseSettled || closed);
					if (!responseSettled && !closed) {
						timedOut = true;
						controller.abort();
					}
				})();
			}

			const response = await fetchPromise.catch((error: unknown) => {
				if (timedOut) {
					throw new OperationalError(
						`Timed out after ${connectionTimeout} ms waiting for the server to respond`,
					);
				}
				throw error;
			});

			if (!response.ok) {
				if (!isRetryableStatus(response.status)) {
					fatalReason = `the server rejected the request with HTTP ${response.status}`;
				}
				throw new OperationalError(
					`Request failed with status ${response.status} ${response.statusText}`,
				);
			}
			if (!response.body) {
				throw new OperationalError('Response did not contain a body to stream');
			}

			attempt = 0;
			serverRetryDelay = undefined;
			reportStatus('connected');

			const reader = response.body.getReader();
			for (;;) {
				const { done, value } = await reader.read();
				if (done) break;
				buffer += decoder.decode(value, { stream: true });
				consumeBuffer();
			}
		};

		const connectionLoop = async () => {
			while (!closed) {
				reportStatus(attempt === 0 ? 'connecting' : 'reconnecting');
				fatalReason = undefined;

				try {
					await readStream();
					if (closed) return;
					reportStatus('disconnected', 'Stream closed by the server');
				} catch (error) {
					if (closed) return;
					reportStatus('error', error instanceof Error ? error.message : String(error));
				}

				buffer = '';
				dataLines = [];
				eventName = '';

				if (fatalReason !== undefined) {
					reportStatus('stopped', `${fatalReason}, so it was not retried`);
					this.emitError(
						new NodeOperationError(
							node,
							`SSE connection to "${url}" cannot proceed because ${fatalReason}`,
							{
								description:
									'Retrying an identical request cannot fix this. Check the URL, the selected credential, and any custom headers.',
							},
						),
					);
					return;
				}

				if (retryConfig === undefined) {
					reportStatus('stopped', 'Auto reconnect is disabled');
					return;
				}

				attempt++;

				if (retryConfig.maxAttempts > 0 && attempt > retryConfig.maxAttempts) {
					reportStatus('stopped', `Gave up after ${retryConfig.maxAttempts} attempts`);
					this.emitError(
						new NodeOperationError(
							node,
							`SSE connection to "${url}" failed after ${retryConfig.maxAttempts} retry attempts`,
						),
					);
					return;
				}

				await waitInSlices(nextRetryDelay(retryConfig), () => closed);
			}
		};

		// The loop owns the connection for the lifetime of the trigger and is intentionally not awaited.
		// Without this catch, an unexpected throw would surface as an unhandled rejection and take the
		// whole n8n process down, stopping every other workflow on the instance too.
		void connectionLoop().catch((error: unknown) => {
			this.logger.error(
				`[Resilient SSE Trigger] ${node.name}: connection loop stopped unexpectedly: ${
					error instanceof Error ? error.message : String(error)
				}`,
				{ url },
			);
		});

		async function closeFunction() {
			closed = true;
			abortController?.abort();
		}

		return {
			closeFunction,
		};
	}
}
