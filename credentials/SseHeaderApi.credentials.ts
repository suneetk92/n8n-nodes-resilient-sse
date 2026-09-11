import type { Icon, ICredentialType, INodeProperties } from 'n8n-workflow';

export class SseHeaderApi implements ICredentialType {
	name = 'sseHeaderApi';

	displayName = 'SSE Header Auth API';

	documentationUrl =
		'https://github.com/suneetk92/n8n-nodes-resilient-sse?tab=readme-ov-file#credentials';

	icon: Icon = {
		light: 'file:../nodes/ResilientSseTrigger/resilient_sse.svg',
		dark: 'file:../nodes/ResilientSseTrigger/resilient_sse.dark.svg',
	};

	properties: INodeProperties[] = [
		{
			displayName: 'Header Name',
			name: 'name',
			type: 'string',
			default: '',
			placeholder: 'X-API-Key',
			description: 'Name of the header carrying the secret',
		},
		// The name-based heuristic cannot tell that this is the secret: for header auth the API key
		// itself goes here, so it must stay masked.
		// eslint-disable-next-line @n8n/community-nodes/credential-unnecessary-password
		{
			displayName: 'Header Value',
			name: 'value',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'Value sent in the header named above, for example the API key itself',
		},
	];
}
