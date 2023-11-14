<?php

declare(strict_types=1);

namespace App\Command;

use App\Factory\GithubRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\DSL\Json;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

use function Flow\ETL\DSL\ref;

#[AsCommand('fetch-data')]
final class FetchCommand extends Command
{
    public function __construct(private readonly string $token)
    {
        if ('' === $token) {
            throw new \InvalidArgumentException('GitHub API Token must be provided.');
        }

        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('org', InputArgument::REQUIRED);
        $this->addArgument('repository', InputArgument::REQUIRED);
        $this->addOption('date-range');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');

        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        (new Flow())
            ->read(new PsrHttpClientDynamicExtractor($client, new GithubRequestFactory($this->token, $org, $repository)))

            // Extract response
            ->withEntry('unpacked', ref('response_body')->jsonDecode())
            ->select('unpacked')

            // Extract data as rows & columns
            ->withEntry('data', ref('unpacked')->expand())
            ->withEntry('data', ref('data')->unpack())
            ->renameAll('data.', '')
            ->drop('unpacked', 'data')

            // Unify key for partitioning
            ->select('created_at', 'user')
            ->withEntry('date_utc', ref('created_at')->toDateTime(\DATE_ATOM)->dateFormat())

            // Save with overwrite, partition files per unified date
            ->mode(SaveMode::Overwrite)
            ->partitionBy(ref('date_utc'))
            ->write(Json::to(__DIR__."/../../warehouse/dev/{$org}/{$repository}/pr"))

            // Execute
            ->run();

        return Command::SUCCESS;
    }
}
