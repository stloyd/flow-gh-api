<?php

declare(strict_types=1);

namespace App\Command;

use App\Factory\GithubRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Window;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\when;

#[AsCommand('report')]
final class ReportCommand extends Command
{
    public function __construct()
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('org', InputArgument::REQUIRED);
        $this->addArgument('repository', InputArgument::REQUIRED);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');

        (new Flow((new ConfigBuilder)->putInputIntoRows()->build()))
            ->read(CSV::from(__DIR__."/../../warehouse/dev/{$org}/{$repository}/pr/date_utc=*"))

//            ->limit(100)

            ->withEntry('count', Window::partitionBy(ref('date_utc'))->rowNumber())
            ->select('_input_file_uri', 'date_utc', 'count')

            ->write(To::output(false))

            // Save with overwrite
//            ->mode(SaveMode::Overwrite)
//            ->write(CSV::to(__DIR__."/../../warehouse/dev/{$org}/{$repository}/report/year.csv"))
            // Execute
            ->run()
        ;

        return Command::SUCCESS;
    }
}
